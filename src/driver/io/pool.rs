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
use rustls::ClientConfig;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, warn};
use parking_lot::{Condvar, Mutex, RwLockReadGuard};

use super::bolt::message_parameters::RouteParameters;
use super::bolt::{BoltData, ResponseCallbacks};
use crate::address::{Address, AddressResolver};
use crate::bookmarks::Bookmarks;
use crate::driver::config::auth::{auth_managers, AuthToken};
use crate::driver::config::AuthConfig;
use crate::driver::RoutingControl;
use crate::error::ServerError;
use crate::sync::MostlyRLock;
use crate::time::Instant;
use crate::{Neo4jError, Result, ValueSend};
use routing::RoutingTable;
pub(crate) use single_pool::SessionAuth;
use single_pool::{SimplePool, SinglePooledBolt, UnpreparedSinglePooledBolt};

// 7 is a reasonable common upper bound for the size of clusters
// this is, however, not a hard limit
const DEFAULT_CLUSTER_SIZE: usize = 7;

#[derive(Debug)]
pub(crate) struct PooledBolt<'pool> {
    bolt: Option<SinglePooledBolt>,
    pool: &'pool Pool,
}

impl<'pool> PooledBolt<'pool> {
    fn wrap_io(&mut self, mut io_op: impl FnMut(&mut Self) -> Result<()>) -> Result<()> {
        let was_broken = self.deref().unexpectedly_closed();
        let res = io_op(self);
        if !was_broken && self.deref().unexpectedly_closed() {
            self.pool.deactivate_server(&self.deref().address())
        }
        res
    }

    #[inline]
    pub(crate) fn read_one(&mut self, deadline: Option<Instant>) -> Result<()> {
        self.wrap_io(|this| {
            let mut cb = Self::new_server_error_handler(self.pool);
            this.bolt
                .as_mut()
                .expect("bolt option should be Some from init to drop")
                .deref_mut()
                .read_one(deadline, Some(&mut cb))
        })
    }

    #[inline]
    pub(crate) fn read_all(&mut self, deadline: Option<Instant>) -> Result<()> {
        self.wrap_io(|this| {
            let mut cb = Self::new_server_error_handler(self.pool);
            this.bolt
                .as_mut()
                .expect("bolt option should be Some from init to drop")
                .deref_mut()
                .read_all(deadline, Some(&mut cb))
        })
    }

    fn new_server_error_handler<RW: Read + Write>(
        pool: &'pool Pool,
    ) -> impl FnMut(&mut BoltData<RW>, &mut ServerError) -> Result<()> + 'pool {
        move |bolt_data, error| pool.handle_server_error(bolt_data, error)
    }

    #[inline]
    pub(crate) fn write_one(&mut self, deadline: Option<Instant>) -> Result<()> {
        self.wrap_io(|this| this.deref_mut().write_one(deadline))
    }

    #[inline]
    pub(crate) fn write_all(&mut self, deadline: Option<Instant>) -> Result<()> {
        self.wrap_io(|this| this.deref_mut().write_all(deadline))
    }
}

impl<'pool> Deref for PooledBolt<'pool> {
    type Target = SinglePooledBolt;

    fn deref(&self) -> &Self::Target {
        self.bolt
            .as_ref()
            .expect("bolt option should be Some from init to drop")
    }
}

impl<'pool> DerefMut for PooledBolt<'pool> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bolt
            .as_mut()
            .expect("bolt option should be Some from init to drop")
    }
}

impl<'pool> Drop for PooledBolt<'pool> {
    fn drop(&mut self) {
        let bolt = self
            .bolt
            .take()
            .expect("bolt option should be Some from init to drop");
        match &self.pool.pools {
            Pools::Direct(_) => drop(bolt),
            Pools::Routing(pool) => {
                let _lock = pool.wait_cond.0.lock();
                drop(bolt);
                pool.wait_cond.1.notify_all();
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PoolConfig {
    pub(crate) routing_context: Option<HashMap<String, ValueSend>>,
    pub(crate) tls_config: Option<Arc<ClientConfig>>,
    pub(crate) user_agent: String,
    pub(crate) auth: AuthConfig,
    pub(crate) max_connection_pool_size: usize,
    pub(crate) connection_timeout: Option<Duration>,
    pub(crate) connection_acquisition_timeout: Option<Duration>,
    pub(crate) resolver: Option<Box<dyn AddressResolver>>,
}

impl PoolConfig {
    pub(crate) fn connection_acquisition_deadline(&self) -> Option<Instant> {
        // Not mocking pool and connection timeouts as this could lead to preemptively giving up:
        // assume time gets frozen at t0, connection_acquisition_timeout passes (for real),
        // then every call to the pool will start to fail because APIs using the actual time are
        // used under the hood.
        self.connection_acquisition_timeout
            .map(|t| Instant::unmockable_now() + t)
    }
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

    pub(crate) fn default_acquisition_deadline(&self) -> Option<Instant> {
        self.config
            .connection_acquisition_timeout
            .map(|t| Instant::now() + t)
    }

    pub(crate) fn resolve_home_db(&self, args: UpdateRtArgs) -> Result<Option<Arc<String>>> {
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
            bolt: Some(match &self.pools {
                Pools::Direct(single_pool) => {
                    let mut connection = None;
                    let deadline = self.config.connection_acquisition_deadline();
                    while connection.is_none() {
                        connection = single_pool.acquire(deadline)?.prepare(
                            deadline,
                            args.update_rt_args.idle_time_before_connection_test,
                            args.update_rt_args.session_auth,
                            None,
                        )?;
                    }
                    connection.expect("loop above asserts existence")
                }
                Pools::Routing(routing_pool) => routing_pool.acquire(args)?,
            }),
            pool: self,
        })
    }

    fn handle_server_error<RW: Read + Write>(
        &self,
        bolt_data: &mut BoltData<RW>,
        error: &mut ServerError,
    ) -> Result<()> {
        match &self.pools {
            Pools::Direct(pool) => handle_server_error(
                PoolsRef::Direct(pool),
                &self.config,
                bolt_data.address(),
                bolt_data.auth(),
                bolt_data.session_auth(),
                error,
            ),
            Pools::Routing(pool) => pool.handle_server_error(bolt_data, error),
        }
    }

    fn deactivate_server(&self, addr: &Address) {
        if let Pools::Routing(routing_pool) = &self.pools {
            routing_pool.deactivate_server(addr)
        }
    }

    fn deactivate_writer(&self, addr: &Address) {
        if let Pools::Routing(routing_pool) = &self.pools {
            routing_pool.deactivate_writer(addr)
        }
    }
}

#[derive(Debug)]
enum Pools {
    Direct(SimplePool),
    Routing(RoutingPool),
}

#[derive(Debug)]
enum PoolsRef<'a> {
    Direct(&'a SimplePool),
    Routing(&'a RoutingPool),
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

type RoutingTables = HashMap<Option<Arc<String>>, RoutingTable>;
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
        let deadline = self.config.connection_acquisition_deadline();
        'target: for target in &targets {
            while let Some(connection) = self.acquire_routing_address_no_wait(target) {
                let mut on_server_error =
                    |bolt_data: &mut _, error: &mut _| self.handle_server_error(bolt_data, error);
                match connection.prepare(
                    deadline,
                    args.update_rt_args.idle_time_before_connection_test,
                    args.update_rt_args.session_auth,
                    Some(&mut on_server_error),
                ) {
                    Ok(Some(connection)) => return Ok(connection),
                    Ok(None) => continue,
                    Err(Neo4jError::Disconnect { .. }) => {
                        self.deactivate_server(target);
                        continue 'target;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        // time to wait for a free connection
        let mut cond_lock = self.wait_cond.0.lock();
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
                let mut on_server_error =
                    |bolt_data: &mut _, error: &mut _| self.handle_server_error(bolt_data, error);
                match connection.prepare(
                    deadline,
                    args.update_rt_args.idle_time_before_connection_test,
                    args.update_rt_args.session_auth,
                    Some(&mut on_server_error),
                ) {
                    Ok(Some(connection)) => return Ok(connection),
                    Ok(None) => {
                        cond_lock = self.wait_cond.0.lock();
                        continue;
                    }
                    Err(Neo4jError::Disconnect { .. }) => {
                        self.deactivate_server(&targets[0]);
                        cond_lock = self.wait_cond.0.lock();
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            match deadline {
                None => self.wait_cond.1.wait(&mut cond_lock),
                Some(timeout) => {
                    if self
                        .wait_cond
                        .1
                        .wait_until(&mut cond_lock, timeout.raw())
                        .timed_out()
                    {
                        return Err(Neo4jError::connection_acquisition_timeout(
                            "waiting for room in the connection pool",
                        ));
                    }
                }
            }
        }
    }

    /// Guarantees that Vec is not empty
    fn choose_addresses_from_fresh_rt(
        &self,
        args: AcquireConfig,
    ) -> Result<(Vec<Arc<Address>>, Option<Arc<String>>)> {
        let (lock, db) = self.get_fresh_rt(args)?;
        let rt = lock
            .get(&db.as_ref().map(Arc::clone))
            .expect("created above");
        Ok((self.servers_by_usage(rt.servers_for_mode(args.mode))?, db))
    }

    /// Guarantees that Vec is not empty
    fn choose_addresses(
        &self,
        args: AcquireConfig,
        db: &Option<Arc<String>>,
    ) -> Result<Vec<Arc<Address>>> {
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

    fn acquire_routing_address(
        &self,
        target: &Arc<Address>,
        args: UpdateRtArgs,
    ) -> Result<SinglePooledBolt> {
        let mut connection = None;
        let deadline = self.config.connection_acquisition_deadline();
        while connection.is_none() {
            let unprepared_connection = {
                let pools = self.ensure_pool_exists(target);
                pools
                    .get(target)
                    .expect("just created above")
                    .acquire(deadline)
            }?;
            let mut on_server_error =
                |bolt_data: &mut _, error: &mut _| self.handle_server_error(bolt_data, error);
            connection = unprepared_connection.prepare(
                deadline,
                args.idle_time_before_connection_test,
                args.session_auth,
                Some(&mut on_server_error),
            )?;
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

    fn get_fresh_rt(
        &self,
        args: AcquireConfig,
    ) -> Result<(RwLockReadGuard<RoutingTables>, Option<Arc<String>>)> {
        let rt_args = args.update_rt_args;
        let db_name = RefCell::new(rt_args.db.map(Arc::clone));
        let db_name_ref = &db_name;
        let lock = self.routing_tables.maybe_write(
            |rts| {
                rts.get(&*db_name_ref.borrow())
                    .map(|rt| !rt.is_fresh(args.mode))
                    .unwrap_or(true)
            },
            |mut rts| {
                let key = rt_args.db.map(Arc::clone);
                let rt = rts.entry(key).or_insert_with(|| self.empty_rt());
                if !rt.is_fresh(args.mode) {
                    let mut new_db = self.update_rts(rt_args, &mut rts)?;
                    if new_db.is_some() && db_name_ref.borrow().is_none() {
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
    ) -> Result<Option<Arc<String>>> {
        let rt_key = args.db.map(Arc::clone);
        let rt = rts.entry(rt_key).or_insert_with(|| self.empty_rt());
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
                    let db = args.db.map(Arc::clone);
                    new_rt.database = db.as_ref().map(Arc::clone);
                    rts.insert(db.as_ref().map(Arc::clone), new_rt);
                    self.clean_up_pools(rts);
                    Ok(db)
                } else {
                    let db = new_rt.database.as_ref().map(Arc::clone);
                    rts.insert(db.as_ref().map(Arc::clone), new_rt);
                    self.clean_up_pools(rts);
                    Ok(db)
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
            for resolution in Arc::clone(router).fully_resolve(self.config.resolver.as_deref())? {
                let Ok(resolved) = resolution else {
                    self.deactivate_server_locked_rts(router, rts);
                    continue;
                };
                match Self::wrap_discovery_error(
                    self.acquire_routing_address(&resolved, args)
                        .and_then(|mut con| self.fetch_rt_from_router(&mut con, args)),
                )? {
                    Ok(rt) => return Ok(Ok(rt)),
                    Err(err) => last_err = Some(err),
                };
                self.deactivate_server_locked_rts(&resolved, rts);
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
            RouteParameters::new(
                self.config.routing_context.as_ref().unwrap(),
                args.bookmarks,
                args.db.as_ref().map(|db| db.as_str()),
                args.imp_user,
            ),
            ResponseCallbacks::new().with_on_success({
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
            }),
        )?;
        con.write_all(None)?;
        con.read_all(
            None,
            Some(&mut |bolt_data, error| {
                if error.unauthenticates_all_connections() {
                    self.reset_all_auth(bolt_data.address());
                }
                Ok(())
            }),
        )?;
        let rt = Arc::try_unwrap(rt).expect("read_all flushes all ResponseCallbacks");
        let rt = rt.into_inner().ok_or_else(|| {
            Neo4jError::protocol_error(
                "server did not reply with SUCCESS or FAILURE to ROUTE request",
            )
        })?;
        if let Ok(rt) = &rt {
            if rt.routers.is_empty() {
                debug!("received routing table without readers -> discarded");
                // It's not technically a disconnect error, but we need to signal that this RT
                // should not be used, the server should be invalidated, and another server, if
                // available, should be tried.
                return Err(Neo4jError::disconnect(
                    "received routing table without readers",
                ));
            }
            if rt.readers.is_empty() {
                debug!("received routing table without readers -> discarded");
                return Err(Neo4jError::disconnect(
                    "received routing table without readers",
                ));
            }
            // If no writers are available, this likely indicates a temporary state, such as leader
            // switching, so we should not signal an error.
        }
        rt
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

    fn deactivate_writer(&self, addr: &Address) {
        drop(self.routing_tables.update(|mut rts| {
            Self::deactivate_writer_locked(addr, &mut rts);
            Ok(())
        }));
    }

    fn deactivate_writer_locked(addr: &Address, rts: &mut RoutingTables) {
        debug!("deactivating writer: {:?}", addr);
        rts.iter_mut()
            .for_each(|(_, rt)| rt.deactivate_writer(addr));
    }

    fn handle_server_error<RW: Read + Write>(
        &self,
        bolt_data: &mut BoltData<RW>,
        error: &mut ServerError,
    ) -> Result<()> {
        handle_server_error(
            PoolsRef::Routing(self),
            &self.config,
            bolt_data.address(),
            bolt_data.auth(),
            bolt_data.session_auth(),
            error,
        )
    }

    fn reset_all_auth(&self, address: &Arc<Address>) {
        self.pools.read().get(address).map(|pool| {
            pool.reset_all_auth();
        });
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

fn handle_server_error(
    pools: PoolsRef,
    config: &Arc<PoolConfig>,
    address: &Arc<Address>,
    current_auth: Option<&Arc<AuthToken>>,
    session_auth: bool,
    error: &mut ServerError,
) -> Result<()> {
    let current_auth = current_auth.ok_or_else(|| {
        Neo4jError::protocol_error("server sent security error over unauthenticated connection")
    })?;
    if error.deactivates_server() {
        match pools {
            PoolsRef::Direct(_) => {}
            PoolsRef::Routing(pool) => pool.deactivate_server(address),
        }
    } else if error.invalidates_writer() {
        match pools {
            PoolsRef::Direct(_) => {}
            PoolsRef::Routing(pool) => pool.deactivate_writer(address),
        }
    }
    if error.is_security_error() {
        if error.unauthenticates_all_connections() {
            debug!(
                "mark all connections to {} as unauthenticated",
                address.deref()
            );
            match pools {
                PoolsRef::Direct(pool) => pool.reset_all_auth(),
                PoolsRef::Routing(pool) => pool.reset_all_auth(address),
            }
        }
        if !session_auth {
            match &config.auth {
                AuthConfig::Static(_) => {}
                AuthConfig::Manager(manager) => {
                    let handled =
                        auth_managers::handle_security_error(&**manager, current_auth, error)?;
                    if handled {
                        error.overwrite_retryable();
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct AcquireConfig<'a> {
    pub(crate) mode: RoutingControl,
    pub(crate) update_rt_args: UpdateRtArgs<'a>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct UpdateRtArgs<'a> {
    pub(crate) db: Option<&'a Arc<String>>,
    pub(crate) bookmarks: Option<&'a Bookmarks>,
    pub(crate) imp_user: Option<&'a str>,
    pub(crate) session_auth: SessionAuth<'a>,
    pub(crate) idle_time_before_connection_test: Option<Duration>,
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
