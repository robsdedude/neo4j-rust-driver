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

pub(crate) mod bookmarks;
pub(crate) mod config;
pub(crate) mod retry;

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;
use std::result::Result as StdResult;
use std::sync::Arc;

use log::{debug, info};

use super::config::auth::AuthToken;
use super::io::bolt::message_parameters::{BeginParameters, RunParameters};
use super::io::{AcquireConfig, Pool, PooledBolt, UpdateRtArgs};
use super::record_stream::RecordStream;
use super::transaction::{Transaction, TransactionTimeout};
use super::{EagerResult, ReducedDriverConfig, RoutingControl};
use crate::driver::io::SessionAuth;
use crate::error_::{Neo4jError, Result};
use crate::transaction::InnerTransaction;
use crate::value::ValueSend;
use bookmarks::{bookmark_managers, BookmarkManager, Bookmarks};
use config::InternalSessionConfig;
pub use config::SessionConfig;
use retry::RetryPolicy;

// imports for docs
#[allow(unused)]
use super::Driver;

/// A session is a container for a series of transactions.
///
/// Sessions, besides being a configuration container, automatically provide
/// [causal chaining](crate#causal-consistency), which means that each transaction can read the
/// results of any previous transaction in the same session.
/// If you need to establish a causal chain between two sessions, you can pass bookmarks manually
/// either by using [`Session::last_bookmarks()`] or by sharing [`BookmarkManager`] instance between
/// sessions (see [`SessionConfig::with_bookmark_manager()`]).
///
/// There are two ways to run a transaction inside a session:
///  * [`Session::transaction()`] runs a normal transaction managed by the client.
///  * [`Session::auto_commit()`] will leave transaction management up to the server.
///    This mode is necessary for certain types of queries that manage their own transactions.
///    Such as `CALL {...} IN TRANSACTION`.
///    This has the big drawback, that the client can easily end up in situations where it's unclear
///    whether a transaction has been committed or not.
///    The only guarantee given is that the transaction has been successfully committed once all
///    results have been consumed.
///
/// See also [`Driver::session()`].
#[derive(Debug)]
pub struct Session<'driver> {
    config: InternalSessionConfig,
    pool: &'driver Pool,
    driver_config: &'driver ReducedDriverConfig,
    resolved_db: Option<Arc<String>>,
    session_bookmarks: SessionBookmarks,
}

impl<'driver> Session<'driver> {
    pub(crate) fn new(
        config: InternalSessionConfig,
        pool: &'driver Pool,
        driver_config: &'driver ReducedDriverConfig,
    ) -> Self {
        let bookmarks = config.config.bookmarks.clone();
        let manager = config.config.as_ref().bookmark_manager.clone();
        Session {
            config,
            pool,
            driver_config,
            resolved_db: None,
            session_bookmarks: SessionBookmarks::new(bookmarks, manager),
        }
    }

    /// Prepare a transaction that will leave transaction management up to the server.
    ///
    /// This mode is necessary for certain types of queries that manage their own transactions.
    /// Such as `CALL {...} IN TRANSACTION`.
    /// This has the big drawback, that the client can easily end up in situations where it's unclear
    /// whether a transaction has been committed or not.
    /// The only guarantee given is that the transaction has been successfully committed once all
    /// results have been consumed.
    ///
    /// Use the returned [`AutoCommitBuilder`] to configure the transaction and run it.
    pub fn auto_commit<'session, Q: AsRef<str>>(
        &'session mut self,
        query: Q,
    ) -> AutoCommitBuilder<
        'driver,
        'session,
        Q,
        DefaultParamKey,
        DefaultParam,
        DefaultMetaKey,
        DefaultMeta,
        DefaultReceiver,
    > {
        AutoCommitBuilder::new(self, query)
    }

    fn auto_commit_run<
        'session,
        Q: AsRef<str>,
        KP: Borrow<str> + Debug,
        P: Borrow<HashMap<KP, ValueSend>>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        R,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
    >(
        &'session mut self,
        builder: AutoCommitBuilder<'driver, 'session, Q, KP, P, KM, M, FRes>,
    ) -> Result<R> {
        let cx = self.acquire_connection(builder.mode)?;
        let mut record_stream =
            RecordStream::new(Rc::new(RefCell::new(cx)), self.fetch_size(), true, None);
        let res = record_stream
            .run(RunParameters::new_auto_commit_run(
                builder.query.as_ref(),
                Some(builder.param.borrow()),
                Some(&*self.session_bookmarks.get_bookmarks_for_work()?),
                builder.timeout.raw(),
                Some(builder.meta.borrow()),
                builder.mode.as_protocol_str(),
                self.resolved_db().as_ref().map(|db| db.as_str()),
                self.config
                    .config
                    .as_ref()
                    .impersonated_user
                    .as_ref()
                    .map(|imp| imp.as_str()),
                &self.config.config.notification_filter,
            ))
            .and_then(|_| (builder.receiver)(&mut record_stream));
        let res = match res {
            Ok(r) => {
                record_stream.consume()?;
                Ok(r)
            }
            Err(e) => {
                let _ = record_stream.consume();
                Err(e)
            }
        };
        let bookmark = record_stream.into_bookmark();
        if let Some(bookmark) = bookmark {
            self.session_bookmarks.update_bookmarks(bookmark)?;
        }
        res
    }

    /// Prepare a transaction.
    ///
    /// Use the returned [`TransactionBuilder`] to configure the transaction and run it.
    pub fn transaction<'session>(
        &'session mut self,
    ) -> TransactionBuilder<'driver, 'session, DefaultMetaKey, DefaultMeta> {
        TransactionBuilder::new(self)
    }

    fn transaction_run<
        'session,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        R,
        FTx: for<'tx> FnOnce(Transaction<'driver, 'tx>) -> Result<R>,
    >(
        &'session mut self,
        builder: &TransactionBuilder<'driver, 'session, KM, M>,
        receiver: FTx,
    ) -> Result<R> {
        let connection = self.acquire_connection(builder.mode)?;
        let mut tx = InnerTransaction::new(connection, self.fetch_size());
        let bookmarks = &*self.session_bookmarks.get_bookmarks_for_work()?;
        let parameters = BeginParameters::new(
            Some(bookmarks),
            builder.timeout.raw(),
            Some(builder.meta.borrow()),
            builder.mode.as_protocol_str(),
            self.resolved_db().as_ref().map(|db| db.as_str()),
            self.config
                .config
                .impersonated_user
                .as_ref()
                .map(|imp| imp.as_str()),
            &self.config.config.notification_filter,
        );
        tx.begin(parameters, self.config.eager_begin)?;
        let res = receiver(Transaction::new(&mut tx));
        let res = match res {
            Ok(_) => {
                tx.close()?;
                res
            }
            Err(_) => {
                if let Err(e) = tx.close() {
                    info!(
                        "while propagating user code error: \
                        ignored tx.close() error in transaction_run: {}",
                        e
                    )
                }
                res
            }
        };
        let bookmark = tx.into_bookmark();
        if let Some(bookmark) = bookmark {
            self.session_bookmarks.update_bookmarks(bookmark)?;
        }
        res
    }

    fn resolve_db(&mut self) -> Result<()> {
        if self.resolved_db().is_none() && self.pool.is_routing() {
            debug!("Resolving home db");
            self.resolved_db = self.pool.resolve_home_db(UpdateRtArgs {
                db: None,
                bookmarks: Some(&*self.session_bookmarks.get_bookmarks_for_work()?),
                imp_user: self
                    .config
                    .config
                    .impersonated_user
                    .as_ref()
                    .map(|imp| imp.as_str()),
                session_auth: self.session_auth(),
                idle_time_before_connection_test: self.config.idle_time_before_connection_test,
            })?;
            debug!("Resolved home db to {:?}", &self.resolved_db);
        }
        Ok(())
    }

    #[inline]
    fn resolved_db(&self) -> &Option<Arc<String>> {
        match self.resolved_db {
            None => &self.config.config.database,
            Some(_) => &self.resolved_db,
        }
    }

    pub(super) fn acquire_connection(
        &mut self,
        mode: RoutingControl,
    ) -> Result<PooledBolt<'driver>> {
        self.resolve_db()?;
        let bookmarks = self.session_bookmarks.get_bookmarks_for_work()?;
        self.pool.acquire(AcquireConfig {
            mode,
            update_rt_args: UpdateRtArgs {
                db: self.resolved_db().as_ref(),
                bookmarks: Some(&*bookmarks),
                imp_user: self
                    .config
                    .config
                    .impersonated_user
                    .as_ref()
                    .map(|imp| imp.as_str()),
                session_auth: self.session_auth(),
                idle_time_before_connection_test: self.config.idle_time_before_connection_test,
            },
        })
    }

    pub(super) fn verify_authentication(&mut self, auth: &Arc<AuthToken>) -> Result<bool> {
        match self.forced_auth(auth) {
            Ok(_) => Ok(true),
            Err(err) => match &err {
                Neo4jError::ServerError { error } => match error.code() {
                    "Neo.ClientError.Security.CredentialsExpired"
                    | "Neo.ClientError.Security.Forbidden"
                    | "Neo.ClientError.Security.TokenExpired"
                    | "Neo.ClientError.Security.Unauthorized" => Ok(false),
                    _ => Err(err),
                },
                Neo4jError::Disconnect { .. }
                | Neo4jError::InvalidConfig { .. }
                | Neo4jError::Timeout { .. }
                | Neo4jError::UserCallback { .. }
                | Neo4jError::ProtocolError { .. } => Err(err),
            },
        }
    }

    fn forced_auth(&mut self, auth: &Arc<AuthToken>) -> Result<()> {
        self.resolve_db()?;
        let bookmarks = self.session_bookmarks.get_bookmarks_for_work()?;
        let mut connection = self.pool.acquire(AcquireConfig {
            mode: RoutingControl::Read,
            update_rt_args: UpdateRtArgs {
                db: self.resolved_db().as_ref(),
                bookmarks: Some(&*bookmarks),
                imp_user: self
                    .config
                    .config
                    .impersonated_user
                    .as_ref()
                    .map(|imp| imp.as_str()),
                session_auth: SessionAuth::Forced(auth),
                idle_time_before_connection_test: self.config.idle_time_before_connection_test,
            },
        })?;
        connection.write_all(None)?;
        connection.read_all(None)
    }

    /// Get the bookmarks last received by the session or the ones it was initialized with.
    ///
    /// This can be used to [causally chain](crate#causal-consistency) together sessions.
    ///
    /// # Example
    /// ```
    /// use std::sync::Arc;
    ///
    /// use neo4j::driver::{Driver, RoutingControl};
    /// use neo4j::session::SessionConfig;
    ///
    /// # use doc_test_utils::get_driver;
    ///
    /// # doc_test_utils::db_exclusive(|| {
    /// let db = Arc::new(String::from("neo4j")); // always specify the database name, if possible
    /// let driver: Driver = get_driver();
    /// let mut session1 = driver.session(SessionConfig::new().with_database(Arc::clone(&db)));
    /// // do work with session1, e.g.,
    /// session1.auto_commit("CREATE (n:Node)").run().unwrap();
    ///
    /// let bookmarks = session1.last_bookmarks();
    /// let mut session2 = driver.session(
    ///     SessionConfig::new()
    ///         .with_bookmarks(bookmarks)
    ///         .with_database(Arc::clone(&db)),
    /// );
    /// // now session2 will see the results of the transaction in session1
    /// let mut result = session2
    ///     .auto_commit("MATCH (n:Node) RETURN count(n)")
    ///     .with_routing_control(RoutingControl::Read)
    ///     .run()
    ///     .unwrap();
    /// assert_eq!(result.into_scalar().unwrap().try_into_int().unwrap(), 1);
    /// # });
    /// ```
    #[inline]
    pub fn last_bookmarks(&self) -> Arc<Bookmarks> {
        self.session_bookmarks.get_current_bookmarks()
    }

    #[inline]
    fn fetch_size(&self) -> i64 {
        self.config
            .config
            .as_ref()
            .fetch_size
            .unwrap_or(self.driver_config.fetch_size)
    }

    #[inline]
    fn session_auth(&self) -> SessionAuth {
        match &self.config.config.auth {
            Some(auth) => SessionAuth::Reauth(auth),
            None => SessionAuth::None,
        }
    }
}

/// Builder type to prepare an auto-commit transaction.
///
/// Use [`Session::auto_commit()`] for creating one and call [`AutoCommitBuilder::run()`]
/// to execute the auto-commit transaction when you're done configuring it.
pub struct AutoCommitBuilder<'driver, 'session, Q, KP, P, KM, M, FRes> {
    session: Option<&'session mut Session<'driver>>,
    query: Q,
    _kp: PhantomData<KP>,
    param: P,
    _km: PhantomData<KM>,
    meta: M,
    timeout: TransactionTimeout,
    mode: RoutingControl,
    receiver: FRes,
}

pub(crate) fn default_receiver(res: &mut RecordStream) -> Result<EagerResult> {
    res.try_as_eager_result().map(|r| {
        r.expect("default receiver does not consume stream before turning it into an eager result")
    })
}

pub(crate) type DefaultReceiver = fn(&mut RecordStream) -> Result<EagerResult>;
pub(crate) type DefaultParamKey = String;
pub(crate) type DefaultParam = HashMap<DefaultParamKey, ValueSend>;
pub(crate) type DefaultMetaKey = String;
pub(crate) type DefaultMeta = HashMap<DefaultMetaKey, ValueSend>;

impl<'driver, 'session, Q: AsRef<str>>
    AutoCommitBuilder<
        'driver,
        'session,
        Q,
        DefaultParamKey,
        DefaultParam,
        DefaultMetaKey,
        DefaultMeta,
        DefaultReceiver,
    >
{
    fn new(session: &'session mut Session<'driver>, query: Q) -> Self {
        Self {
            session: Some(session),
            query,
            _kp: PhantomData,
            param: Default::default(),
            _km: PhantomData,
            meta: Default::default(),
            timeout: Default::default(),
            mode: RoutingControl::Write,
            receiver: default_receiver,
        }
    }
}

impl<
        'driver,
        'session,
        Q: AsRef<str>,
        KP: Borrow<str> + Debug,
        P: Borrow<HashMap<KP, ValueSend>>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        R,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
    > AutoCommitBuilder<'driver, 'session, Q, KP, P, KM, M, FRes>
{
    /// Configure query parameters.
    ///
    /// # Example
    /// ```
    /// use neo4j::{value_map, ValueReceive};
    ///
    /// # doc_test_utils::db_exclusive(|| {
    /// # let driver = doc_test_utils::get_driver();
    /// # let mut session = doc_test_utils::get_session(&driver);
    /// let mut result = session
    ///     .auto_commit("CREATE (n:Node {id: $id}) RETURN n")
    ///     .with_parameters(value_map!({"id": 1}))
    ///     .run()
    ///     .unwrap();
    /// let mut node = result.into_scalar().unwrap().try_into_node().unwrap();
    /// assert_eq!(node.properties.remove("id").unwrap(), ValueReceive::Integer(1));
    /// # });
    /// ```
    ///
    /// Always prefer this over query string manipulation to avoid injection vulnerabilities and to
    /// allow the server to cache the query plan.
    #[inline]
    pub fn with_parameters<KP_: Borrow<str> + Debug, P_: Borrow<HashMap<KP_, ValueSend>>>(
        self,
        param: P_,
    ) -> AutoCommitBuilder<'driver, 'session, Q, KP_, P_, KM, M, FRes> {
        let Self {
            session,
            query,
            _kp: _,
            param: _,
            _km,
            meta,
            timeout,
            mode,
            receiver,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            _kp: PhantomData,
            param,
            _km,
            meta,
            timeout,
            mode,
            receiver,
        }
    }

    /// Configure the query to not use any parameters.
    ///
    /// This is the *default*.
    #[inline]
    pub fn without_parameters(
        self,
    ) -> AutoCommitBuilder<'driver, 'session, Q, DefaultParamKey, DefaultParam, KM, M, FRes> {
        let Self {
            session,
            query,
            _kp: _,
            param: _,
            _km,
            meta,
            timeout,
            mode,
            receiver,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            _kp: PhantomData,
            param: Default::default(),
            _km,
            meta,
            timeout,
            mode,
            receiver,
        }
    }

    /// Attach transaction metadata to the query.
    ///
    /// See [`TransactionBuilder::with_transaction_meta()`] for more information.
    ///
    /// # Example
    /// ```
    /// use neo4j::value_map;
    ///
    /// # let driver = doc_test_utils::get_driver();
    /// # let mut session = doc_test_utils::get_session(&driver);
    /// let result = session
    ///    .auto_commit("MATCH (n:Node) RETURN n")
    ///    .with_transaction_meta(value_map!({"key": "value"}))
    ///    .run()
    ///    .unwrap();
    /// ```
    #[inline]
    pub fn with_transaction_meta<KM_: Borrow<str> + Debug, M_: Borrow<HashMap<KM_, ValueSend>>>(
        self,
        meta: M_,
    ) -> AutoCommitBuilder<'driver, 'session, Q, KP, P, KM_, M_, FRes> {
        let Self {
            session,
            query,
            _kp,
            param,
            _km: _,
            meta: _,
            timeout,
            mode,
            receiver,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            _kp,
            param,
            _km: PhantomData,
            meta,
            timeout,
            mode,
            receiver,
        }
    }

    /// Configure the query to not use any transaction metadata.
    ///
    /// This is the *default*.
    #[inline]
    pub fn without_transaction_meta(
        self,
    ) -> AutoCommitBuilder<'driver, 'session, Q, KP, P, DefaultMetaKey, DefaultMeta, FRes> {
        let Self {
            session,
            query,
            _kp,
            param,
            _km: _,
            meta: _,
            timeout,
            mode,
            receiver,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            _kp,
            param,
            _km: PhantomData,
            meta: Default::default(),
            timeout,
            mode,
            receiver,
        }
    }

    /// Instruct the server to abort the transaction after the given timeout.
    ///
    /// See [`TransactionTimeout`] for options.
    #[inline]
    pub fn with_transaction_timeout(mut self, timeout: TransactionTimeout) -> Self {
        self.timeout = timeout;
        self
    }

    /// Specify whether the query should be send to a reader or writer in the cluster.
    ///
    /// See [`TransactionBuilder::with_routing_control()`] for more information.
    #[inline]
    pub fn with_routing_control(mut self, mode: RoutingControl) -> Self {
        self.mode = mode;
        self
    }

    /// Specify a custom receiver to handle the result stream.
    ///
    /// By default ([`AutoCommitBuilder::with_default_receiver()`]), the result stream will be
    /// collected into memory and returned as [`EagerResult`].
    ///
    /// # Example
    /// ```
    /// use neo4j::driver::record_stream::RecordStream;
    /// use neo4j::driver::Record;
    ///
    /// # let driver = doc_test_utils::get_driver();
    /// # let mut session = doc_test_utils::get_session(&driver);
    /// let sum = session
    ///     .auto_commit("UNWIND range(1, 3) AS x RETURN x")
    ///     .with_receiver(|stream: &mut RecordStream| {
    ///         let mut sum = 0;
    ///         for result in stream {
    ///             let mut record: Record = result?;
    ///             sum += record.into_values().next().unwrap().try_into_int().unwrap();
    ///         }
    ///         Ok(sum)
    ///     })
    ///     .run()
    ///     .unwrap();
    ///
    /// assert_eq!(sum, 6);
    /// ```
    #[inline]
    pub fn with_receiver<R_, FRes_: FnOnce(&mut RecordStream) -> Result<R_>>(
        self,
        receiver: FRes_,
    ) -> AutoCommitBuilder<'driver, 'session, Q, KP, P, KM, M, FRes_> {
        let Self {
            session,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            receiver: _,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            receiver,
        }
    }

    /// Set the receiver back to the default, which will collect the result stream into memory and
    /// return it as [`EagerResult`].
    #[inline]
    pub fn with_default_receiver(
        self,
    ) -> AutoCommitBuilder<'driver, 'session, Q, KP, P, KM, M, DefaultReceiver> {
        let Self {
            session,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            receiver: _,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            _kp,
            param,
            _km,
            meta,
            timeout,
            mode,
            receiver: default_receiver,
        }
    }

    /// Run the query and return the result.
    pub fn run(mut self) -> Result<R> {
        let session = self.session.take().unwrap();
        session.auto_commit_run(self)
    }
}

impl<
        'driver,
        'session,
        Q: AsRef<str>,
        KP: Borrow<str> + Debug,
        P: Borrow<HashMap<KP, ValueSend>>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        FRes,
    > Debug for AutoCommitBuilder<'driver, 'session, Q, KP, P, KM, M, FRes>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoCommitBuilder")
            .field(
                "session",
                &match self.session {
                    None => "None",
                    Some(_) => "Some(...)",
                },
            )
            .field("query", &self.query.as_ref())
            .field("param", &self.param.borrow())
            .field("meta", &self.meta.borrow())
            .field("timeout", &self.timeout)
            .field("mode", &self.mode)
            .field("receiver", &"...")
            .finish()
    }
}

/// Builder type to prepare a transaction.
///
/// Use [`Session::transaction()`] for creating one and call [`TransactionBuilder::run()`]
/// to execute the transaction when you're done configuring it.
pub struct TransactionBuilder<'driver, 'session, KM, M> {
    session: Option<&'session mut Session<'driver>>,
    _km: PhantomData<KM>,
    meta: M,
    timeout: TransactionTimeout,
    mode: RoutingControl,
}

impl<'driver, 'session> TransactionBuilder<'driver, 'session, DefaultMetaKey, DefaultMeta> {
    fn new(session: &'session mut Session<'driver>) -> Self {
        Self {
            session: Some(session),
            _km: PhantomData,
            meta: Default::default(),
            timeout: Default::default(),
            mode: RoutingControl::Write,
        }
    }
}

impl<'driver, 'session, KM: Borrow<str> + Debug, M: Borrow<HashMap<KM, ValueSend>>>
    TransactionBuilder<'driver, 'session, KM, M>
{
    /// Attach transaction metadata to the transaction.
    ///
    /// Transaction metadata will be logged in the server's `query.log` and is accessible through
    /// querying `SHOW TRANSACTIONS YIELD *`.
    /// Metadata can also manually be set via the `dbms.setTXMetaData` procedure.
    ///
    /// # Example
    /// ```
    /// # use neo4j::driver::EagerResult;
    /// use neo4j::transaction::Transaction;
    /// # use neo4j::Result;
    /// use neo4j::value_map;
    ///
    /// # let driver = doc_test_utils::get_driver();
    /// # let mut session = doc_test_utils::get_session(&driver);
    ///
    /// let result = session
    ///     .transaction()
    ///     .with_transaction_meta(value_map!({"key": "value"}))
    ///     .run(|tx: Transaction| {
    ///         // ...
    ///         # tx.query("MATCH (n:Node) RETURN n").run()?.try_as_eager_result()?;
    ///         # tx.commit()
    ///     })
    ///     .unwrap();
    /// ```
    #[inline]
    pub fn with_transaction_meta<KM_: Borrow<str> + Debug, M_: Borrow<HashMap<KM_, ValueSend>>>(
        self,
        meta: M_,
    ) -> TransactionBuilder<'driver, 'session, KM_, M_> {
        let Self {
            session,
            _km: _,
            meta: _,
            timeout,
            mode,
        } = self;
        TransactionBuilder {
            session,
            _km: PhantomData,
            meta,
            timeout,
            mode,
        }
    }

    /// Configure the transaction to not use any transaction metadata (this is default).
    #[inline]
    pub fn without_transaction_meta(
        self,
    ) -> TransactionBuilder<'driver, 'session, DefaultMetaKey, DefaultMeta> {
        let Self {
            session,
            _km: _,
            meta: _,
            timeout,
            mode,
        } = self;
        TransactionBuilder {
            session,
            _km: PhantomData,
            meta: Default::default(),
            timeout,
            mode,
        }
    }

    /// Instruct the server to abort the transaction after the given timeout.
    ///
    /// See [`TransactionTimeout`] for options.
    #[inline]
    pub fn with_transaction_timeout(mut self, timeout: TransactionTimeout) -> Self {
        self.timeout = timeout;
        self
    }

    /// Specify whether the query should be send to a reader or writer in the cluster.
    ///
    /// Writers (*default*), can handle reads and writes.
    /// However, when running read-only queries, it's more efficient to send them to a reader to
    /// avoid overloading the writer.
    ///
    /// **Writers** are also known as **leaders** or **primaries**.  
    /// **Readers** are also known as **followers** or **secondaries** as well as
    /// **read replicas** or **tertiaries**.
    #[inline]
    pub fn with_routing_control(mut self, mode: RoutingControl) -> Self {
        self.mode = mode;
        self
    }

    /// Run the transaction. The work to be done is specified by the given `receiver`.
    ///
    /// The `receiver` will be called with a [`Transaction`] that can be used to execute queries,
    /// and control the transaction (commit, rollback, ...).
    ///
    /// Especially when running against a clustered or cloud-hosted DBMS, it's recommended to use
    /// [`TransactionBuilder::run_with_retry()`] over this method because many intermittent errors
    /// can occur in such cases (e.g., leader switches, connections killed by load balancers, ...).
    ///
    /// # Example
    /// ```
    /// use std::sync::Arc;
    ///
    /// use neo4j::driver::EagerResult;
    /// use neo4j::transaction::Transaction;
    /// use neo4j::Result;
    /// use neo4j::{value_map, ValueReceive};
    ///
    /// # use doc_test_utils::get_session;
    /// #
    /// # doc_test_utils::db_exclusive(|| {
    /// # let driver = doc_test_utils::get_driver();
    /// #
    /// // always specify the database name, if possible
    /// let database = Arc::new(String::from("neo4j"));
    ///
    /// // populate database
    /// driver
    ///     .execute_query("UNWIND range(1, 3) AS x CREATE (n:Actor {fame: x})")
    ///     .with_database(Arc::clone(&database))
    ///     .run()
    ///     .unwrap();
    ///
    /// let total_fame = get_session(&driver)
    ///     .transaction()
    ///     .run(|tx: Transaction| {
    ///         let actors = tx.query("MATCH (n:Actor) RETURN n").run()?;
    ///         let mut total_fame = 0;
    ///         for result in actors {
    ///             let mut record = result?;
    ///             let mut actor = record.into_values().next().unwrap().try_into_node().unwrap();
    ///             let fame: ValueReceive = actor.properties.remove("fame").unwrap();
    ///             let fame: i64 = fame.try_into_int().unwrap();
    ///             total_fame += fame * 2;
    ///             // increase everyone's fame!
    ///             tx.query("MATCH (n:Actor) WHERE id(n) = $id SET n.fame = $fame")
    ///                 .with_parameters(value_map!({"id": actor.id, "fame": fame * 2}))
    ///                 .run()?;
    ///             // ...
    ///             // NOTE: this is just for demonstration purposes, in reality you would
    ///             //       do this in a single query and save many round trips to the server:
    ///             //       MATCH (n:Actor) SET n.fame = n.fame * 2
    ///         }
    ///         // now attempt commit the whole transaction
    ///         tx.commit()?;
    ///         // to roll it back, you can either call `tx.rollback()` or `drop` the Transaction
    ///         Ok(total_fame) // return any value you want from the transaction
    ///     })
    ///     .unwrap();
    ///
    /// assert_eq!(total_fame, 12);
    /// let db_total_fame = driver
    ///     .execute_query("MATCH (n:Actor) RETURN sum(n.fame) AS total_fame")
    ///     .with_database(Arc::clone(&database))
    ///     .run()
    ///     .unwrap()
    ///     .into_scalar()
    ///     .unwrap();
    /// assert_eq!(db_total_fame, ValueReceive::Integer(total_fame));
    /// # });
    /// ```
    pub fn run<R>(mut self, receiver: impl FnOnce(Transaction) -> Result<R>) -> Result<R> {
        let session = self.session.take().unwrap();
        session.transaction_run(&self, receiver)
    }

    /// Run the transaction with a retry policy.
    ///
    /// This is pretty much the same as [`TransactionBuilder::run()`], except that the `receiver`
    /// will be retried if it returns an error deemed retryable by the given `retry_policy`.
    ///
    /// See also [`RetryPolicy`].
    pub fn run_with_retry<R, P: RetryPolicy>(
        mut self,
        retry_policy: P,
        mut receiver: impl FnMut(Transaction) -> Result<R>,
    ) -> StdResult<R, P::Error> {
        let session = self.session.take().unwrap();
        retry_policy.execute(|| session.transaction_run(&self, &mut receiver))
    }
}

impl<'driver, 'session, KM, M: Debug> Debug for TransactionBuilder<'driver, 'session, KM, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBuilder")
            .field(
                "session",
                &match self.session {
                    None => "None",
                    Some(_) => "Some(...)",
                },
            )
            .field("meta", &self.meta)
            .field("timeout", &self.timeout)
            .field("mode", &self.mode)
            .finish()
    }
}

#[derive(Debug)]
enum SessionBookmarks {
    Unmanaged {
        bookmarks: Arc<Bookmarks>,
    },
    ManagedInit {
        bookmarks: Arc<Bookmarks>,
        manager: Arc<dyn BookmarkManager>,
    },
    ManagedGet {
        bookmarks: Arc<Bookmarks>,
        previous_bookmarks: Arc<Bookmarks>,
        manager: Arc<dyn BookmarkManager>,
    },
    ManagedUpdated {
        bookmarks: Arc<Bookmarks>,
        previous_bookmarks: Arc<Bookmarks>,
        manager: Arc<dyn BookmarkManager>,
    },
}

impl SessionBookmarks {
    fn new(bookmarks: Option<Arc<Bookmarks>>, manager: Option<Arc<dyn BookmarkManager>>) -> Self {
        match manager {
            None => Self::Unmanaged {
                bookmarks: bookmarks.unwrap_or_default(),
            },
            Some(manager) => Self::ManagedInit {
                bookmarks: bookmarks.unwrap_or_default(),
                manager,
            },
        }
    }

    fn get_current_bookmarks(&self) -> Arc<Bookmarks> {
        match &self {
            Self::Unmanaged { bookmarks }
            | Self::ManagedInit { bookmarks, .. }
            | Self::ManagedGet { bookmarks, .. }
            | Self::ManagedUpdated { bookmarks, .. } => Arc::clone(bookmarks),
        }
    }

    fn get_bookmarks_for_work(&mut self) -> Result<Arc<Bookmarks>> {
        match self {
            Self::Unmanaged { bookmarks } => Ok(Arc::clone(bookmarks)),
            Self::ManagedInit { bookmarks, manager }
            | Self::ManagedGet {
                bookmarks, manager, ..
            } => {
                let manager_bookmarks = bookmark_managers::get_bookmarks(&**manager)?;
                let previous_bookmarks = Arc::new(&*manager_bookmarks + &**bookmarks);
                *self = Self::ManagedGet {
                    bookmarks: Arc::clone(bookmarks),
                    previous_bookmarks: Arc::clone(&previous_bookmarks),
                    manager: Arc::clone(manager),
                };
                Ok(previous_bookmarks)
            }
            Self::ManagedUpdated {
                manager,
                previous_bookmarks,
                ..
            } => {
                *previous_bookmarks = bookmark_managers::get_bookmarks(&**manager)?;
                Ok(Arc::clone(previous_bookmarks))
            }
        }
    }

    fn update_bookmarks(&mut self, bookmark: String) -> Result<()> {
        match self {
            SessionBookmarks::Unmanaged { bookmarks } => {
                *bookmarks = Arc::new(Bookmarks::from_raw([bookmark]));
            }
            SessionBookmarks::ManagedInit { .. } => {
                panic!("Cannot update bookmarks before first get")
            }
            SessionBookmarks::ManagedGet {
                bookmarks,
                previous_bookmarks,
                manager,
            } => {
                *bookmarks = Arc::new(Bookmarks::from_raw([bookmark]));
                bookmark_managers::update_bookmarks(
                    &**manager,
                    Arc::clone(previous_bookmarks),
                    Arc::clone(bookmarks),
                )?;
                *self = Self::ManagedUpdated {
                    bookmarks: Arc::clone(bookmarks),
                    previous_bookmarks: Arc::clone(previous_bookmarks),
                    manager: Arc::clone(manager),
                };
            }
            SessionBookmarks::ManagedUpdated {
                bookmarks,
                previous_bookmarks,
                manager,
            } => {
                *bookmarks = Arc::new(Bookmarks::from_raw([bookmark]));
                bookmark_managers::update_bookmarks(
                    &**manager,
                    Arc::clone(previous_bookmarks),
                    Arc::clone(bookmarks),
                )?;
            }
        }
        Ok(())
    }
}
