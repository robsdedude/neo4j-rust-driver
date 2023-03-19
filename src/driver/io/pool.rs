// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod routing;
mod single_pool;

use atomic_refcell::AtomicRefCell;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::mem::{swap, ManuallyDrop};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::result;
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard};

use itertools::Itertools;
use log::{debug, info, warn};

use super::bolt::{self, ResponseCallbacks, TcpBolt};
use crate::sync::MostlyRLock;
use crate::{Address, Neo4jError, Result, RoutingControl, Value};
use routing::RoutingTable;
use single_pool::SinglePool;

pub(crate) use single_pool::PooledBolt;

#[derive(Debug)]
pub(crate) struct PoolConfig {
    pub(crate) address: Arc<Address>,
    // pub(crate) routing: bool,
    pub(crate) routing_context: Option<HashMap<String, Value>>,
    // pub(crate) ssl_context: Option<SslContext>,
    pub(crate) user_agent: String,
    pub(crate) auth: HashMap<String, Value>,
    pub(crate) max_connection_pool_size: usize,
}

#[derive(Debug)]
pub(crate) struct Pool {
    config: Arc<PoolConfig>,
    pools: Pools,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        let config = Arc::new(config);
        Self {
            config: Arc::clone(&config),
            pools: Pools::new(config),
        }
    }

    pub(crate) fn is_routing(&self) -> bool {
        self.config.routing_context.is_some()
    }

    pub(crate) fn resolve_home_db(&self) -> Result<Option<String>> {
        let Pools::Routing(pools) = &self.pools else {
            panic!("don't call resolve_home_db on a direct pool")
        };
        let mut resolved_db = None;
        {
            let resolved_db = &mut resolved_db;
            drop(pools.routing_tables.update(move |mut rts| {
                *resolved_db = pools.update_rts(&None, &mut rts)?.clone();
                Ok(())
            }));
        }
        Ok(resolved_db)
    }

    pub(crate) fn acquire(&self, args: AcquireConfig) -> Result<PooledBolt> {
        match &self.pools {
            Pools::Direct(single_pool) => single_pool.acquire(),
            Pools::Routing(routing_pool) => routing_pool.acquire(args),
        }
    }
}

#[derive(Debug)]
enum Pools {
    Direct(SinglePool),
    Routing(RoutingPool),
}

impl Pools {
    fn new(config: Arc<PoolConfig>) -> Self {
        match config.routing_context {
            None => Pools::Direct(SinglePool::new(config)),
            Some(_) => Pools::Routing(RoutingPool::new(config)),
        }
    }

    pub(crate) fn is_routing(&self) -> bool {
        matches!(self, Pools::Routing(_))
    }

    pub(crate) fn is_direct(&self) -> bool {
        matches!(self, Pools::Direct(_))
    }
}

type RoutingTables = HashMap<Option<String>, RoutingTable>;
type RoutingPools = HashMap<Address, SinglePool>;

#[derive(Debug)]
struct RoutingPool {
    pools: MostlyRLock<RoutingPools>,
    routing_tables: MostlyRLock<RoutingTables>,
    config: Arc<PoolConfig>,
}

impl RoutingPool {
    fn new(config: Arc<PoolConfig>) -> Self {
        assert!(config.routing_context.is_some());
        Self {
            pools: MostlyRLock::new(HashMap::with_capacity(
                // 7 is a reasonable common upper bound for the size of clusters
                7,
            )),
            routing_tables: MostlyRLock::new(HashMap::new()),
            config,
        }
    }

    fn acquire(&self, args: AcquireConfig) -> Result<PooledBolt> {
        let target = self.choose_address(args)?;
        self.acquire_routing_address(&target)
    }

    fn choose_address(&self, args: AcquireConfig) -> Result<Arc<Address>> {
        let AcquireConfig { db, mode, .. } = args;
        let rts = self.get_fresh_rts(args)?;
        let rt = rts.get(db).expect("just created above");
        self.least_used_server(rt.servers_for_mode(args.mode))
    }

    fn acquire_routing_address(&self, target: &Address) -> Result<PooledBolt> {
        let pools = self.pools.maybe_write(
            |rt| rt.get(target).is_none(),
            |mut rt| {
                rt.insert((*target).clone(), SinglePool::new(Arc::clone(&self.config)));
                Ok(())
            },
        )?;
        pools.get(&target).expect("just created above").acquire()
    }

    fn get_fresh_rts(&self, args: AcquireConfig) -> Result<RwLockReadGuard<RoutingTables>> {
        self.routing_tables.maybe_write(
            |rts| {
                rts.get(args.db)
                    .map(|rt| rt.is_fresh(args.mode))
                    .unwrap_or(true)
            },
            |mut rts| {
                if rts.get(args.db).is_none() {
                    rts.insert(args.db.clone(), self.empty_rt());
                }
                let rt = rts.get(args.db).unwrap();
                if !rt.is_fresh(args.mode) {
                    self.update_rts(args.db, &mut rts)?;
                }
                Ok(())
            },
        )
    }

    fn least_used_server(&self, addresses: &[Arc<Address>]) -> Result<Arc<Address>> {
        Ok(Arc::clone(match addresses.len() {
            0 => return Err(Neo4jError::disconnect("routing options depleted")),
            1 => &addresses[0],
            _ => {
                let pools = self.pools.read();
                addresses
                    .into_iter()
                    .map(|addr| (addr, pools.get(&addr).map(|p| p.in_use()).unwrap_or(0)))
                    .min_by_key(|(_, usage)| *usage)
                    .expect("cannot be of size 0")
                    .0
            }
        }))
    }

    fn update_rts<'a>(
        &self,
        db: &Option<String>,
        rts: &'a mut RoutingTables,
    ) -> Result<&'a Option<String>> {
        // fixme: give up when server returns invalid RT
        let rt = match rts.get(db) {
            None => {
                rts.insert(db.clone(), self.empty_rt());
                rts.get(db).expect("inserted above")
            }
            Some(rt) => rt,
        };
        let pref_init_router = rt.initialized_without_writers;
        let mut new_rt: Option<RoutingTable>;
        let routers = rt
            .routers
            .iter()
            .filter(|&r| r != &self.config.address)
            .map(Arc::clone)
            .collect::<Vec<_>>();
        if pref_init_router {
            new_rt = self.fetch_rt_from_routers(&[&self.config.address], db, rts)?;
            if new_rt.is_none() {
                new_rt = self.fetch_rt_from_routers(&routers, db, rts)?;
            }
        } else {
            new_rt = self.fetch_rt_from_routers(&routers, db, rts)?;
            if new_rt.is_none() {
                new_rt = self.fetch_rt_from_routers(&[&self.config.address], db, rts)?;
            }
        }
        match new_rt {
            None => Err(Neo4jError::disconnect(
                "unable to retrieve routing information",
            )),
            Some(mut new_rt) => {
                if db.is_some() {
                    new_rt.database = db.clone();
                }
                rts.insert(new_rt.database.clone(), new_rt);
                Ok(&rts.get(db).expect("inserted above").database)
            }
        }
    }

    fn fetch_rt_from_routers<ADDR: AsRef<Address>>(
        &self,
        routers: &[ADDR],
        db: &Option<String>,
        rts: &mut RoutingTables,
    ) -> Result<Option<RoutingTable>> {
        for router in routers.iter().map(AsRef::as_ref) {
            if router.is_resolved {
                let Some(mut con) = Self::wrap_discovery_error(self.acquire_routing_address( router))? else {
                    self.deactivate_server(router, rts);
                    continue;
                };
                let Ok(rt) = self.fetch_rt_from_router(&mut con, db) else {
                    self.deactivate_server(router, rts);
                    continue;
                };
                todo!("save rt")
            }
            let Ok(ip_addresses) = router.resolve() else {
                self.deactivate_server(router, rts);
                continue;
            };
            for ip_address in ip_addresses {}
        }
        todo!()
    }

    fn fetch_rt_from_router(
        &self,
        con: &mut PooledBolt,
        db: &Option<String>,
    ) -> Result<RoutingTable> {
        let rt = Arc::new(AtomicRefCell::new(None));
        con.route(
            self.config.routing_context.as_ref().unwrap(),
            None, // TODO: bookmarks
            db.as_deref(),
            None, // TODO: impersonation
            ResponseCallbacks::new().with_on_success({
                let rt = Arc::clone(&rt);
                move |meta| {
                    let new_rt = RoutingTable::try_parse(meta);
                    let mut res;
                    match new_rt {
                        Ok(new_rt) => res = Some(Ok(new_rt)),
                        Err(e) => {
                            warn!("failed to parse routing table: {}", e);
                            res = Some(Err(Neo4jError::ProtocolError {
                                message: format!("{}", e),
                            }));
                        }
                    }
                    swap(rt.deref().borrow_mut().deref_mut(), &mut res);
                    Ok(())
                }
            }),
        )?;
        con.write_all()?;
        con.read_all()?;
        let rt = Arc::try_unwrap(rt).map_err(|_| Neo4jError::ProtocolError {
            message: String::from("server did not respond to ROUTE request"),
        })?;
        rt.into_inner().ok_or_else(|| Neo4jError::ProtocolError {
            message: String::from("server did not reply with SUCCESS to ROUTE request"),
        })?
    }

    fn empty_rt(&self) -> RoutingTable {
        RoutingTable::new(Arc::clone(&self.config.address))
    }

    fn deactivate_server(&self, addr: &Address, rts: &mut RoutingTables) {
        debug!("deactivating address: {:?}", addr);
        rts.iter_mut().for_each(|(_, rt)| rt.deactivate(addr));
        drop(self.pools.update(|mut pools| {
            pools.remove(addr);
            Ok(())
        }));
    }

    fn wrap_discovery_error<T>(res: Result<T>) -> Result<Option<T>> {
        match res {
            Ok(t) => Ok(Some(t)),
            Err(e) => {
                if e.fatal_during_discovery() {
                    Err(e)
                } else {
                    info!("ignored error during discovery: {:?}", e);
                    Ok(None)
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct AcquireConfig<'a> {
    pub(crate) mode: RoutingControl,
    pub(crate) db: &'a Option<String>,
}
