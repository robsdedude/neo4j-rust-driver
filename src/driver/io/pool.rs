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
use itertools::Itertools;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex, RwLockReadGuard};

use log::{debug, info, warn};

use super::bolt::{ResponseCallbacks, TcpBolt};
use crate::driver::RoutingControl;
use crate::error::ServerError;
use crate::sync::MostlyRLock;
use crate::{Address, Neo4jError, Result, ValueSend};
use routing::RoutingTable;
use single_pool::{SimplePool, SinglePooledBolt, UnpreparedSinglePooledBolt};

// 7 is a reasonable common upper bound for the size of clusters
// this is, however, not a hard limit
const DEFAULT_CLUSTER_SIZE: usize = 7;

#[derive(Debug)]
pub(crate) struct PooledBolt<'pool> {
    bolt: ManuallyDrop<SinglePooledBolt>,
    pool: &'pool Pool,
}

impl<'pool> PooledBolt<'pool> {
    fn wrap_io(&mut self, io_op: fn(&mut TcpBolt) -> Result<()>) -> Result<()> {
        let was_broken = self.bolt.unexpectedly_closed();
        let res = io_op(&mut self.bolt);
        if !was_broken && self.bolt.unexpectedly_closed() {
            self.pool.deactivate_server(&self.bolt.address())
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

impl<'pool> Drop for PooledBolt<'pool> {
    fn drop(&mut self) {
        // safety: we're not using ManuallyDrop after this call
        let bolt;
        unsafe {
            bolt = ManuallyDrop::take(&mut self.bolt);
        }
        match &self.pool.pools {
            Pools::Direct(_) => drop(bolt),
            Pools::Routing(pool) => {
                let _lock = pool.wait_cond.0.lock().unwrap();
                drop(bolt);
                pool.wait_cond.1.notify_all()
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PoolConfig {
    // pub(crate) routing: bool,
    pub(crate) routing_context: Option<HashMap<String, ValueSend>>,
    // pub(crate) ssl_context: Option<SslContext>,
    pub(crate) user_agent: String,
    pub(crate) auth: HashMap<String, ValueSend>,
    pub(crate) max_connection_pool_size: usize,
}

#[derive(Debug)]
pub(crate) struct Pool {
    address: Arc<Address>,
    config: Arc<PoolConfig>,
    pools: Pools,
}

impl Pool {
    pub(crate) fn new(address: Arc<Address>, config: PoolConfig) -> Self {
        let config = Arc::new(config);
        let pools = Pools::new(Arc::clone(&address), Arc::clone(&config));
        Self {
            address,
            config,
            pools,
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
        Ok(PooledBolt {
            bolt: ManuallyDrop::new(match &self.pools {
                Pools::Direct(single_pool) => {
                    let mut connection = None;
                    while connection.is_none() {
                        connection = single_pool.acquire().prepare()?;
                    }
                    connection.expect("loop above asserts existence")
                }
                Pools::Routing(routing_pool) => routing_pool.acquire(args)?,
            }),
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
    Direct(SimplePool),
    Routing(RoutingPool),
}

impl Pools {
    fn new(address: Arc<Address>, config: Arc<PoolConfig>) -> Self {
        match config.routing_context {
            None => Pools::Direct(SimplePool::new(address, config)),
            Some(_) => Pools::Routing(RoutingPool::new(address, config)),
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
type RoutingPools = HashMap<Arc<Address>, SimplePool>;

#[derive(Debug)]
struct RoutingPool {
    pools: MostlyRLock<RoutingPools>,
    wait_cond: Arc<(Mutex<()>, Condvar)>,
    routing_tables: MostlyRLock<RoutingTables>,
    address: Arc<Address>,
    config: Arc<PoolConfig>,
}

impl RoutingPool {
    fn new(address: Arc<Address>, config: Arc<PoolConfig>) -> Self {
        assert!(config.routing_context.is_some());
        Self {
            pools: MostlyRLock::new(HashMap::with_capacity(DEFAULT_CLUSTER_SIZE)),
            wait_cond: Arc::new((Mutex::new(()), Condvar::new())),
            routing_tables: MostlyRLock::new(HashMap::new()),
            address,
            config,
        }
    }

    fn acquire(&self, args: AcquireConfig) -> Result<SinglePooledBolt> {
        let (mut targets, db) = self.choose_addresses_from_fresh_rt(args)?;
        for target in &targets {
            while let Some(connection) = self.acquire_routing_address_no_wait(target) {
                match connection.prepare() {
                    Ok(Some(connection)) => return Ok(connection),
                    Ok(None) => continue,
                    Err(Neo4jError::Disconnect { .. }) => {
                        self.deactivate_server(&targets[0]);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // time to wait for a free connection
        let mut cond_lock = self.wait_cond.0.lock().unwrap();
        loop {
            targets = self.choose_addresses(args, &db)?;
            // a connection could've been returned while we didn't hold the lock
            // => try again with the lock
            let connection = targets
                .iter()
                .map(|target| self.acquire_routing_address_no_wait(target))
                .skip_while(Option::is_none)
                .map(Option::unwrap)
                .next();
            if let Some(connection) = connection {
                drop(cond_lock);
                match connection.prepare() {
                    Ok(Some(connection)) => return Ok(connection),
                    Ok(None) => {
                        cond_lock = self.wait_cond.0.lock().unwrap();
                        continue;
                    }
                    Err(Neo4jError::Disconnect { .. }) => {
                        self.deactivate_server(&targets[0]);
                        cond_lock = self.wait_cond.0.lock().unwrap();
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            cond_lock = self.wait_cond.1.wait(cond_lock).unwrap();
        }
    }

    /// Guarantees that Vec is not empty
    fn choose_addresses_from_fresh_rt<'a>(
        &self,
        args: AcquireConfig<'a>,
    ) -> Result<(Vec<Arc<Address>>, DbName<'a>)> {
        let (lock, db) = self.get_fresh_rt(args)?;
        let rt = lock.get(&*db).expect("created above");
        Ok((self.servers_by_usage(rt.servers_for_mode(args.mode))?, db))
    }

    /// Guarantees that Vec is not empty
    fn choose_addresses(&self, args: AcquireConfig, db: &DbName) -> Result<Vec<Arc<Address>>> {
        let rts = self.routing_tables.read();
        self.servers_by_usage(
            rts.get(db)
                .map(|rt| rt.servers_for_mode(args.mode))
                .unwrap_or(&[]),
        )
    }

    fn acquire_routing_address_no_wait(
        &self,
        target: &Arc<Address>,
    ) -> Option<UnpreparedSinglePooledBolt> {
        let pools = self.ensure_pool_exists(target);
        pools
            .get(target)
            .expect("just created above")
            .acquire_no_wait()
    }

    fn acquire_routing_address(&self, target: &Arc<Address>) -> Result<SinglePooledBolt> {
        let mut connection = None;
        while connection.is_none() {
            connection = {
                let pools = self.ensure_pool_exists(target);
                pools.get(target).expect("just created above").acquire()
            }
            .prepare()?
        }
        Ok(connection.expect("loop above asserts existence"))
    }

    fn ensure_pool_exists(&self, target: &Arc<Address>) -> RwLockReadGuard<RoutingPools> {
        self.pools
            .maybe_write(
                |rt| rt.get(target).is_none(),
                |mut rt| {
                    rt.insert(
                        Arc::clone(target),
                        SimplePool::new(Arc::clone(target), Arc::clone(&self.config)),
                    );
                    Ok(())
                },
            )
            .expect("updater is infallible")
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
                let rt = rts.get(rt_args.db).expect("inserted above");
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

    /// Guarantees that Vec is not empty
    fn servers_by_usage(&self, addresses: &[Arc<Address>]) -> Result<Vec<Arc<Address>>> {
        Ok(match addresses.len() {
            0 => return Err(Neo4jError::disconnect("routing options depleted")),
            1 => vec![Arc::clone(&addresses[0])],
            _ => {
                let pools = self.pools.read();
                addresses
                    .iter()
                    .map(|addr| (addr, pools.get(addr).map(|p| p.in_use()).unwrap_or(0)))
                    .sorted_unstable_by_key(|(_, usage)| *usage)
                    .map(|(addr, _)| Arc::clone(addr))
                    // .rev()
                    .collect()
            }
        })
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
            .filter(|&r| r != &self.address)
            .map(Arc::clone)
            .collect::<Vec<_>>();
        if pref_init_router {
            new_rt = self.fetch_rt_from_routers(&[Arc::clone(&self.address)], args, rts)?;
            if new_rt.is_err() && !routers.is_empty() {
                new_rt = self.fetch_rt_from_routers(&routers, args, rts)?;
            }
        } else {
            new_rt = self.fetch_rt_from_routers(&routers, args, rts)?;
            if new_rt.is_err() {
                new_rt = self.fetch_rt_from_routers(&[Arc::clone(&self.address)], args, rts)?;
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
                    self.clean_up_pools(rts);
                    Ok(&rts.get(args.db).expect("inserted above").database)
                } else {
                    // can get rid of the clone once
                    // https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html#method.insert_entry
                    // has been stabilized
                    let db = new_rt.database.clone();
                    rts.insert(new_rt.database.clone(), new_rt);
                    self.clean_up_pools(rts);
                    Ok(&rts.get(&db).expect("inserted above").database)
                }
            }
        }
    }

    fn fetch_rt_from_routers(
        &self,
        routers: &[Arc<Address>],
        args: UpdateRtArgs,
        rts: &mut RoutingTables,
    ) -> Result<Result<RoutingTable>> {
        let mut last_err = None;
        for router in routers {
            if router.is_resolved {
                match Self::wrap_discovery_error(
                    self.acquire_routing_address(router)
                        .and_then(|mut con| self.fetch_rt_from_router(&mut con, args)),
                )? {
                    Ok(rt) => return Ok(Ok(rt)),
                    Err(err) => last_err = Some(err),
                };
                self.deactivate_server_locked_rts(router, rts);
            } else {
                let Ok(resolved_addresses) = router.resolve() else {
                self.deactivate_server_locked_rts(router, rts);
                continue;
            };
                for resolved_address in resolved_addresses.into_iter().map(Arc::new) {
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
                                res = Some(Err(Neo4jError::protocol_error(format!("{}", e))));
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
        rt.into_inner().ok_or_else(|| {
            Neo4jError::protocol_error("server did not reply with SUCCESS to ROUTE request")
        })?
    }

    fn empty_rt(&self) -> RoutingTable {
        RoutingTable::new(Arc::clone(&self.address))
    }

    fn clean_up_pools(&self, rts: &mut RoutingTables) {
        drop(self.pools.update(|mut pools| {
            let used_addresses = rts
                .values()
                .map(|rt| {
                    [&rt.readers, &rt.routers, &rt.writers]
                        .into_iter()
                        .flat_map(|addrs| addrs.iter().map(Arc::clone))
                        .collect::<Vec<_>>()
                })
                .fold(
                    HashSet::with_capacity(DEFAULT_CLUSTER_SIZE),
                    |mut set, addrs| {
                        addrs.into_iter().for_each(|addr| {
                            set.insert(addr);
                        });
                        set
                    },
                );
            let existing_addresses = pools
                .iter()
                .map(|(addr, _)| addr.clone())
                .collect::<HashSet<_>>();
            for addr in existing_addresses {
                if !used_addresses.contains(&addr) {
                    pools.remove(&addr);
                }
            }
            Ok(())
        }));
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
            DbName::Ref(s) => s,
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
