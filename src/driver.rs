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

pub(crate) mod config;
pub(crate) mod eager_result;
pub(crate) mod io;
pub(crate) mod record;
pub mod record_stream;
pub(crate) mod session;
pub(crate) mod summary;
pub mod transaction;

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use crate::bookmarks::{bookmark_managers, BookmarkManager};
use crate::{Result, ValueSend};
use config::auth::AuthToken;
pub use config::{
    ConfigureFetchSizeError, ConnectionConfig, ConnectionConfigParseError, DriverConfig,
    TlsConfigError,
};
pub use eager_result::EagerResult;
use io::{AcquireConfig, Pool, PoolConfig, PooledBolt, SessionAuth, UpdateRtArgs};
pub use record::Record;
use record_stream::RecordStream;
use session::config::InternalSessionConfig;
use session::retry::RetryPolicy;
use session::{
    default_receiver, DefaultMeta, DefaultMetaKey, DefaultParam, DefaultParamKey, DefaultReceiver,
    Session, SessionConfig,
};
use summary::ServerInfo;
use transaction::TransactionTimeout;

pub mod auth {
    pub use super::config::auth::*;
}

/// The driver hold the configuration and connection pool to your Neo4j DBMS.
///
/// Main ways to run work against the DBMS through the driver are:
///  * [`Driver::execute_query()`] for running a single query inside a transaction.
///  * [`Driver::session()`] for several mechanisms offering more advance patterns.
#[derive(Debug)]
pub struct Driver {
    pub(crate) config: ReducedDriverConfig,
    pub(crate) pool: Pool,
    capability_check_config: SessionConfig,
    execute_query_bookmark_manager: Arc<dyn BookmarkManager>,
}

impl Driver {
    /// Create a new driver.
    ///
    /// **Note:**  
    /// Driver creation is *lazy*.
    /// No connections are established until work is performed.
    /// If you want to verify connectivity, use [`Driver::verify_connectivity`].
    pub fn new(mut connection_config: ConnectionConfig, config: DriverConfig) -> Self {
        if let Some(routing_context) = &mut connection_config.routing_context {
            let before = routing_context.insert(
                String::from("address"),
                connection_config.address.to_string().into(),
            );
            assert!(
                before.is_none(),
                "address was already set in routing context"
            );
        }
        let pool_config = PoolConfig {
            routing_context: connection_config.routing_context,
            tls_config: connection_config.tls_config.map(Arc::new),
            user_agent: config.user_agent,
            auth: config.auth,
            max_connection_pool_size: config.max_connection_pool_size,
            connection_timeout: config.connection_timeout,
            connection_acquisition_timeout: config.connection_acquisition_timeout,
            resolver: config.resolver,
        };
        Driver {
            config: ReducedDriverConfig {
                fetch_size: config.fetch_size,
                idle_time_before_connection_test: config.idle_time_before_connection_test,
            },
            pool: Pool::new(Arc::new(connection_config.address), pool_config),
            capability_check_config: SessionConfig::default()
                .with_database(Arc::new(String::from("system"))),
            execute_query_bookmark_manager: Arc::new(bookmark_managers::simple(None)),
        }
    }

    pub fn session(&self, config: SessionConfig) -> Session {
        let config = InternalSessionConfig {
            config,
            idle_time_before_connection_test: self.config.idle_time_before_connection_test,
            eager_begin: true,
        };
        Session::new(config, &self.pool, &self.config)
    }

    fn execute_query_session(
        &self,
        database: Option<Arc<String>>,
        impersonated_user: Option<Arc<String>>,
        auth: Option<Arc<AuthToken>>,
        bookmark_manager: ExecuteQueryBookmarkManager,
    ) -> Session {
        let mut session_config = SessionConfig::new();
        session_config.database = database;
        session_config.impersonated_user = impersonated_user;
        session_config.auth = auth;
        session_config.bookmark_manager = match &bookmark_manager {
            ExecuteQueryBookmarkManager::None => None,
            ExecuteQueryBookmarkManager::DriverDefault => {
                Some(Arc::clone(&self.execute_query_bookmark_manager))
            }
            ExecuteQueryBookmarkManager::Custom(manager) => Some(Arc::clone(manager)),
        };
        let config = InternalSessionConfig {
            config: session_config,
            idle_time_before_connection_test: self.config.idle_time_before_connection_test,
            eager_begin: false,
        };
        Session::new(config, &self.pool, &self.config)
    }

    pub fn execute_query<Q: AsRef<str>>(
        &self,
        query: Q,
    ) -> ExecuteQueryBuilder<
        Q,
        DefaultParamKey,
        DefaultParam,
        DefaultMetaKey,
        DefaultMeta,
        DefaultReceiver,
    > {
        ExecuteQueryBuilder::new(self, query)
    }

    pub fn execute_query_bookmark_manager(&self) -> Arc<dyn BookmarkManager> {
        Arc::clone(&self.execute_query_bookmark_manager)
    }

    pub fn verify_connectivity(&self) -> Result<()> {
        self.acquire_connectivity_checked().map(|_| ())
    }

    pub fn get_server_info(&self) -> Result<ServerInfo> {
        self.acquire_connectivity_checked()
            .map(|connection| ServerInfo::new(&connection))
    }

    pub fn verify_authentication(&self, auth: Arc<AuthToken>) -> Result<bool> {
        self.session(self.capability_check_config.clone())
            .verify_authentication(&auth)
    }

    fn acquire_connectivity_checked(&self) -> Result<PooledBolt> {
        let config = InternalSessionConfig {
            config: SessionConfig::default(),
            idle_time_before_connection_test: Some(Duration::ZERO),
            eager_begin: true,
        };
        Session::new(config, &self.pool, &self.config)
            .acquire_connection(RoutingControl::Read)
            .and_then(|mut con| {
                con.write_all(None)?;
                con.read_all(None)?;
                Ok(con)
            })
    }

    pub fn supports_multi_db(&self) -> Result<bool> {
        self.acquire_capability_check_connection()
            .map(|connection| connection.protocol_version() >= (4, 0))
    }

    pub fn supports_session_auth(&self) -> Result<bool> {
        self.acquire_capability_check_connection()
            .map(|connection| connection.protocol_version() >= (5, 1))
    }

    fn acquire_capability_check_connection(&self) -> Result<PooledBolt> {
        self.pool.acquire(AcquireConfig {
            mode: RoutingControl::Read,
            update_rt_args: UpdateRtArgs {
                db: self.capability_check_config.database.as_ref(),
                bookmarks: None,
                imp_user: None,
                session_auth: SessionAuth::None,
                idle_time_before_connection_test: None,
            },
        })
    }
}

#[derive(Debug)]
enum ExecuteQueryBookmarkManager {
    None,
    DriverDefault,
    Custom(Arc<dyn BookmarkManager>),
}

pub struct ExecuteQueryBuilder<'driver, Q, KP, P, KM, M, FRes> {
    driver: &'driver Driver,
    query: Q,
    _kp: PhantomData<KP>,
    param: P,
    _km: PhantomData<KM>,
    meta: M,
    timeout: TransactionTimeout,
    mode: RoutingControl,
    database: Option<Arc<String>>,
    impersonated_user: Option<Arc<String>>,
    auth: Option<Arc<AuthToken>>,
    bookmark_manager: ExecuteQueryBookmarkManager,
    receiver: FRes,
}

impl<'driver, Q: AsRef<str>>
    ExecuteQueryBuilder<
        'driver,
        Q,
        DefaultParamKey,
        DefaultParam,
        DefaultMetaKey,
        DefaultMeta,
        DefaultReceiver,
    >
{
    fn new(driver: &'driver Driver, query: Q) -> Self {
        Self {
            driver,
            query,
            _kp: PhantomData,
            param: Default::default(),
            _km: PhantomData,
            meta: Default::default(),
            timeout: Default::default(),
            mode: RoutingControl::Write,
            database: None,
            impersonated_user: None,
            bookmark_manager: ExecuteQueryBookmarkManager::DriverDefault,
            auth: None,
            receiver: default_receiver,
        }
    }
}

impl<
        'driver,
        Q: AsRef<str>,
        KP: Borrow<str> + Debug,
        P: Borrow<HashMap<KP, ValueSend>>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        R,
        FRes: FnMut(&mut RecordStream) -> Result<R>,
    > ExecuteQueryBuilder<'driver, Q, KP, P, KM, M, FRes>
{
    #[inline]
    pub fn with_parameters<KP_: Borrow<str> + Debug, P_: Borrow<HashMap<KP_, ValueSend>>>(
        self,
        param: P_,
    ) -> ExecuteQueryBuilder<'driver, Q, KP_, P_, KM, M, FRes> {
        let Self {
            driver,
            query,
            _kp: _,
            param: _,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        } = self;
        ExecuteQueryBuilder {
            driver,
            query,
            _kp: PhantomData,
            param,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        }
    }

    #[inline]
    pub fn without_parameters(
        self,
    ) -> ExecuteQueryBuilder<'driver, Q, DefaultParamKey, DefaultParam, KM, M, FRes> {
        let Self {
            driver,
            query,
            _kp: _,
            param: _,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        } = self;
        ExecuteQueryBuilder {
            driver,
            query,
            _kp: PhantomData,
            param: Default::default(),
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        }
    }

    #[inline]
    pub fn with_transaction_meta<KM_: Borrow<str> + Debug, M_: Borrow<HashMap<KM_, ValueSend>>>(
        self,
        meta: M_,
    ) -> ExecuteQueryBuilder<'driver, Q, KP, P, KM_, M_, FRes> {
        let Self {
            driver,
            query,
            _kp,
            param,
            _km: _,
            meta: _,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        } = self;
        ExecuteQueryBuilder {
            driver,
            query,
            _kp,
            param,
            _km: PhantomData,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        }
    }

    #[inline]
    pub fn without_transaction_meta(
        self,
    ) -> ExecuteQueryBuilder<'driver, Q, KP, P, DefaultMetaKey, DefaultMeta, FRes> {
        let Self {
            driver,
            query,
            _kp,
            param,
            _km: _,
            meta: _,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        } = self;
        ExecuteQueryBuilder {
            driver,
            query,
            _kp,
            param,
            _km: PhantomData,
            meta: Default::default(),
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        }
    }

    #[inline]
    pub fn with_transaction_timeout(mut self, timeout: TransactionTimeout) -> Self {
        self.timeout = timeout;
        self
    }

    #[inline]
    pub fn without_transaction_timeout(mut self) -> Self {
        self.timeout = TransactionTimeout::none();
        self
    }

    #[inline]
    pub fn with_default_transaction_timeout(mut self) -> Self {
        self.timeout = TransactionTimeout::default();
        self
    }

    #[inline]
    pub fn with_routing_control(mut self, mode: RoutingControl) -> Self {
        self.mode = mode;
        self
    }

    #[inline]
    pub fn with_database(mut self, database: Arc<String>) -> Self {
        self.database = Some(database);
        self
    }

    #[inline]
    pub fn with_default_database(mut self) -> Self {
        self.database = None;
        self
    }

    #[inline]
    pub fn with_impersonated_user(mut self, user: Arc<String>) -> Self {
        self.impersonated_user = Some(user);
        self
    }

    #[inline]
    pub fn without_impersonated_user(mut self) -> Self {
        self.impersonated_user = None;
        self
    }

    #[inline]
    pub fn with_session_auth(mut self, auth: Arc<AuthToken>) -> Self {
        self.auth = Some(auth);
        self
    }

    #[inline]
    pub fn without_session_auth(mut self) -> Self {
        self.auth = None;
        self
    }

    #[inline]
    pub fn with_bookmark_manager(mut self, manager: Arc<dyn BookmarkManager>) -> Self {
        self.bookmark_manager = ExecuteQueryBookmarkManager::Custom(manager);
        self
    }

    #[inline]
    pub fn with_default_bookmark_manager(mut self) -> Self {
        self.bookmark_manager = ExecuteQueryBookmarkManager::DriverDefault;
        self
    }

    #[inline]
    pub fn without_bookmark_manager(mut self) -> Self {
        self.bookmark_manager = ExecuteQueryBookmarkManager::None;
        self
    }

    #[inline]
    pub fn with_receiver<R_, FRes_: FnOnce(&mut RecordStream) -> Result<R_>>(
        self,
        receiver: FRes_,
    ) -> ExecuteQueryBuilder<'driver, Q, KP, P, KM, M, FRes_> {
        let Self {
            driver,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver: _,
        } = self;
        ExecuteQueryBuilder {
            driver,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver,
        }
    }

    #[inline]
    pub fn with_default_receiver(
        self,
    ) -> ExecuteQueryBuilder<'driver, Q, KP, P, KM, M, DefaultReceiver> {
        let Self {
            driver,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver: _,
        } = self;
        ExecuteQueryBuilder {
            driver,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            bookmark_manager,
            auth,
            receiver: default_receiver,
        }
    }

    pub fn run(self) -> Result<R> {
        let Self {
            driver,
            query,
            _kp: _,
            param,
            _km: _,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            auth,
            bookmark_manager,
            mut receiver,
        } = self;
        let mut session =
            driver.execute_query_session(database, impersonated_user, auth, bookmark_manager);
        let tx_builder = session
            .transaction()
            .with_transaction_meta(meta.borrow())
            .with_transaction_timeout(timeout)
            .with_routing_control(mode);
        tx_builder.run(move |tx| {
            let mut result_stream = tx.query(query).with_parameters(param).run()?;
            let res = receiver(result_stream.raw_stream_mut())?;
            result_stream.consume()?;
            tx.commit()?;
            Ok(res)
        })
    }

    pub fn run_with_retry<RP: RetryPolicy>(self, retry_policy: RP) -> StdResult<R, RP::Error> {
        let Self {
            driver,
            query,
            _kp: _,
            param,
            _km: _,
            meta,
            timeout,
            mode,
            database,
            impersonated_user,
            auth,
            bookmark_manager,
            mut receiver,
        } = self;
        let mut session =
            driver.execute_query_session(database, impersonated_user, auth, bookmark_manager);
        let tx_builder = session
            .transaction()
            .with_transaction_meta(meta.borrow())
            .with_transaction_timeout(timeout)
            .with_routing_control(mode);
        tx_builder.run_with_retry(retry_policy, move |tx| {
            let mut result_stream = tx
                .query(query.as_ref())
                .with_parameters(param.borrow())
                .run()?;
            let res = receiver(result_stream.raw_stream_mut())?;
            result_stream.consume()?;
            tx.commit()?;
            Ok(res)
        })
    }
}

impl<
        'driver,
        Q: AsRef<str>,
        KP: Borrow<str> + Debug,
        P: Borrow<HashMap<KP, ValueSend>>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        FRes,
    > Debug for ExecuteQueryBuilder<'driver, Q, KP, P, KM, M, FRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteQueryBuilder")
            .field("driver", &"...")
            .field("query", &self.query.as_ref())
            .field("param", &self.param.borrow())
            .field("meta", &self.meta.borrow())
            .field("timeout", &self.timeout)
            .field("mode", &self.mode)
            .field("database", &self.database)
            .field("impersonated_user", &self.impersonated_user)
            .field("auth", &self.auth)
            .field("bookmark_manager", &self.bookmark_manager)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct ReducedDriverConfig {
    pub(crate) fetch_size: i64,
    pub(crate) idle_time_before_connection_test: Option<Duration>,
}

#[derive(Debug, Copy, Clone)]
pub enum RoutingControl {
    Read,
    Write,
}

impl RoutingControl {
    pub(crate) fn as_protocol_str(&self) -> Option<&'static str> {
        match self {
            RoutingControl::Read => Some("r"),
            RoutingControl::Write => None,
        }
    }
}
