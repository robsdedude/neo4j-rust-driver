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
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLockReadGuard};

use log::{debug, info, warn};

use super::bolt::{ResponseCallbacks, TcpBolt};
use crate::driver::RoutingControl;
use crate::error::ServerError;
use crate::sync::MostlyRLock;
use crate::{Address, Neo4jError, Result, ValueSend};
use routing::RoutingTable;
use single_pool::{SinglePool, SinglePooledBolt};

#[derive(Debug)]
pub(crate) struct PooledBolt<'pool> {
    bolt: SinglePooledBolt,
    pool: &'pool Pool,
}

impl<'pool> PooledBolt<'pool> {
    fn wrap_io(&mut self, io_op: fn(&mut TcpBolt) -> Result<()>) -> Result<()> {
        let was_broken = self.bolt.unexpectedly_closed();
        let res = io_op(&mut self.bolt);
        if !was_broken && self.bolt.unexpectedly_closed() {
            self.pool.deactivate_server(self.bolt.address())
        }
        res
    }

    #[inline]
    pub(crate) fn read_one(&mut self) -> Result<()> {
        self.wrap_io(TcpBolt::read_one)
    }

    #[inline]
    pub(crate) fn read_all(&mut self) -> Result<()> {
        self.wrap_io(TcpBolt::read_all)
    }

    #[inline]
    pub(crate) fn write_one(&mut self) -> Result<()> {
        self.wrap_io(TcpBolt::write_one)
    }

    #[inline]
    pub(crate) fn write_all(&mut self) -> Result<()> {
        self.wrap_io(TcpBolt::write_all)
    }
}

impl<'pool> Deref for PooledBolt<'pool> {
    type Target = SinglePooledBolt;

    fn deref(&self) -> &Self::Target {
        &self.bolt
    }
}

impl<'pool> DerefMut for PooledBolt<'pool> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bolt
    }
}

#[derive(Debug)]
pub(crate) struct PoolConfig {
    pub(crate) address: Arc<Address>,
    // pub(crate) routing: bool,
    pub(crate) routing_context: Option<HashMap<String, ValueSend>>,
    // pub(crate) ssl_context: Option<SslContext>,
    pub(crate) user_agent: String,
    pub(crate) auth: HashMap<String, ValueSend>,
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

    pub(crate) fn resolve_home_db(&self, args: UpdateRtArgs) -> Result<Option<String>> {
        let Pools::Routing(pools) = &self.pools else {
            panic!("don't call resolve_home_db on a direct pool")
        };
        if args.db.is_some() {
            panic!("don't call resolve_home_db with a database")
        }
        let mut resolved_db = None;
        {
            let resolved_db = &mut resolved_db;
            drop(pools.routing_tables.update(move |mut rts| {
                *resolved_db = pools.update_rts(args, &mut rts)?.clone();
                Ok(())
            })?);
        }
        Ok(resolved_db)
    }

    pub(crate) fn acquire(&self, args: AcquireConfig) -> Result<PooledBolt> {
        match &self.pools {
            Pools::Direct(single_pool) => single_pool.acquire(),
            Pools::Routing(routing_pool) => routing_pool.acquire(args),
        }
        .map(|connection| PooledBolt {
            bolt: connection,
            pool: self,
        })
    }

    fn deactivate_server(&self, addr: &Address) {
        if let Pools::Routing(routing_pool) = &self.pools {
            routing_pool.deactivate_server(addr)
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

    fn acquire(&self, args: AcquireConfig) -> Result<SinglePooledBolt> {
        let target = self.choose_address(args)?;
        self.acquire_routing_address(&target)
    }

    fn choose_address(&self, args: AcquireConfig) -> Result<Arc<Address>> {
        let (lock, db) = self.get_fresh_rt(args)?;
        let rt = lock.get(&*db).expect("created above");
        self.least_used_server(rt.servers_for_mode(args.mode))
    }

    fn acquire_routing_address(&self, target: &Address) -> Result<SinglePooledBolt> {
        let pools = self.pools.maybe_write(
            |rt| rt.get(target).is_none(),
            |mut rt| {
                rt.insert((*target).clone(), SinglePool::new(Arc::clone(&self.config)));
                Ok(())
            },
        )?;
        pools.get(target).expect("just created above").acquire()
    }

    fn get_fresh_rt<'lock, 'db>(
        &'lock self,
        args: AcquireConfig<'db>,
    ) -> Result<(RwLockReadGuard<'lock, RoutingTables>, DbName<'db>)> {
        let rt_args = args.update_rt_args;
        let db_name = RefCell::new(DbName::Ref(rt_args.db));
        let db_name_ref = &db_name;
        let lock = self.routing_tables.maybe_write(
            |rts| {
                rts.get(&*db_name_ref.borrow())
                    .map(|rt| !rt.is_fresh(args.mode))
                    .unwrap_or(true)
            },
            |mut rts| {
                if rts.get(rt_args.db).is_none() {
                    rts.insert(rt_args.db.clone(), self.empty_rt());
                }
                let rt = rts.get(rt_args.db).unwrap();
                if !rt.is_fresh(args.mode) {
                    let new_db = self.update_rts(rt_args, &mut rts)?;
                    if db_name_ref.borrow().is_none() && new_db.is_some() {
                        let mut new_db = DbName::Owned(new_db.clone());
                        mem::swap(&mut *db_name_ref.borrow_mut(), &mut new_db);
                    }
                }
                Ok(())
            },
        )?;
        Ok((lock, db_name.into_inner()))
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
        args: UpdateRtArgs,
        rts: &'a mut RoutingTables,
    ) -> Result<&'a Option<String>> {
        let rt = match rts.get(args.db) {
            None => {
                rts.insert(args.db.clone(), self.empty_rt());
                rts.get(args.db).expect("inserted above")
            }
            Some(rt) => rt,
        };
        let pref_init_router = rt.initialized_without_writers;
        let mut new_rt: Result<RoutingTable>;
        let routers = rt
            .routers
            .iter()
            .filter(|&r| r != &self.config.address)
            .map(Arc::clone)
            .collect::<Vec<_>>();
        if pref_init_router {
            new_rt = self.fetch_rt_from_routers(&[&self.config.address], args, rts)?;
            if new_rt.is_err() && !routers.is_empty() {
                new_rt = self.fetch_rt_from_routers(&routers, args, rts)?;
            }
        } else {
            new_rt = self.fetch_rt_from_routers(&routers, args, rts)?;
            if new_rt.is_err() {
                new_rt = self.fetch_rt_from_routers(&[&self.config.address], args, rts)?;
            }
        }
        match new_rt {
            Err(err) => Err(Neo4jError::disconnect(format!(
                "unable to retrieve routing information; last error: {}",
                err
            ))),
            Ok(mut new_rt) => {
                if args.db.is_some() {
                    new_rt.database = args.db.clone();
                    rts.insert(new_rt.database.clone(), new_rt);
                    Ok(&rts.get(args.db).expect("inserted above").database)
                } else {
                    // can get rid of the clone once
                    // https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html#method.insert_entry
                    // has been stabilized
                    let db = new_rt.database.clone();
                    rts.insert(new_rt.database.clone(), new_rt);
                    Ok(&rts.get(&db).expect("inserted above").database)
                }
            }
        }
    }

    fn fetch_rt_from_routers<ADDR: AsRef<Address>>(
        &self,
        routers: &[ADDR],
        args: UpdateRtArgs,
        rts: &mut RoutingTables,
    ) -> Result<Result<RoutingTable>> {
        let mut last_err = None;
        for router in routers.iter().map(AsRef::as_ref) {
            if router.is_resolved {
                match Self::wrap_discovery_error(
                    self.acquire_routing_address(router)
                        .and_then(|mut con| self.fetch_rt_from_router(&mut con, args)),
                )? {
                    Ok(rt) => return Ok(Ok(rt)),
                    Err(err) => last_err = Some(err),
                };
                self.deactivate_server_locked_rts(&router, rts);
            } else {
                let Ok(resolved_addresses) = router.resolve() else {
                self.deactivate_server_locked_rts(router, rts);
                continue;
            };
                for resolved_address in resolved_addresses {
                    match Self::wrap_discovery_error(
                        self.acquire_routing_address(&resolved_address)
                            .and_then(|mut con| self.fetch_rt_from_router(&mut con, args)),
                    )? {
                        Ok(rt) => return Ok(Ok(rt)),
                        Err(err) => last_err = Some(err),
                    };
                    self.deactivate_server_locked_rts(&resolved_address, rts);
                }
            }
        }
        Ok(Err(match last_err {
            None => Neo4jError::disconnect("no known routers left"),
            Some(err) => err,
        }))
    }

    fn fetch_rt_from_router(
        &self,
        con: &mut SinglePooledBolt,
        args: UpdateRtArgs,
    ) -> Result<RoutingTable> {
        let rt = Arc::new(AtomicRefCell::new(None));
        con.route(
            self.config.routing_context.as_ref().unwrap(),
            args.bookmarks.as_deref(),
            args.db.as_deref(),
            args.imp_user.as_deref(),
            ResponseCallbacks::new()
                .with_on_success({
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
                        mem::swap(rt.deref().borrow_mut().deref_mut(), &mut res);
                        Ok(())
                    }
                })
                .with_on_failure(|meta| Err(ServerError::from_meta(meta).into())),
        )?;
        con.write_all()?;
        con.read_all()?;
        let rt = Arc::try_unwrap(rt).expect("read_all flushes all ResponseCallbacks");
        rt.into_inner().ok_or_else(|| Neo4jError::ProtocolError {
            message: String::from("server did not reply with SUCCESS to ROUTE request"),
        })?
    }

    fn empty_rt(&self) -> RoutingTable {
        RoutingTable::new(Arc::clone(&self.config.address))
    }

    fn deactivate_server(&self, addr: &Address) {
        drop(self.routing_tables.update(|mut rts| {
            drop(self.pools.update(|mut pools| {
                Self::deactivate_server_locked(addr, &mut rts, &mut pools);
                Ok(())
            }));
            Ok(())
        }));
    }

    fn deactivate_server_locked_rts(&self, addr: &Address, rts: &mut RoutingTables) {
        drop(self.pools.update(|mut pools| {
            Self::deactivate_server_locked(addr, rts, &mut pools);
            Ok(())
        }));
    }

    fn deactivate_server_locked(addr: &Address, rts: &mut RoutingTables, pools: &mut RoutingPools) {
        debug!("deactivating address: {:?}", addr);
        rts.iter_mut().for_each(|(_, rt)| rt.deactivate(addr));
        pools.remove(addr);
    }

    fn wrap_discovery_error<T>(res: Result<T>) -> Result<Result<T>> {
        match res {
            Ok(t) => Ok(Ok(t)),
            Err(e) => {
                if e.fatal_during_discovery() {
                    Err(e)
                } else {
                    info!("ignored error during discovery: {:?}", e);
                    Ok(Err(e))
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct AcquireConfig<'a> {
    pub(crate) mode: RoutingControl,
    pub(crate) update_rt_args: UpdateRtArgs<'a>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct UpdateRtArgs<'a> {
    pub(crate) db: &'a Option<String>,
    pub(crate) bookmarks: &'a Option<Vec<String>>,
    pub(crate) imp_user: &'a Option<String>,
}

enum DbName<'a> {
    Ref(&'a Option<String>),
    Owned(Option<String>),
}

impl<'a> Deref for DbName<'a> {
    type Target = Option<String>;

    fn deref(&self) -> &Self::Target {
        match self {
            DbName::Ref(s) => *s,
            DbName::Owned(s) => s,
        }
    }
}

impl<'a> DbName<'a> {
    fn into_key(self) -> Option<String> {
        match self {
            DbName::Ref(s) => s.clone(),
            DbName::Owned(s) => s,
        }
    }
}
