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

use log::{debug, info};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;
use std::result;

use thiserror::Error;

use super::io::bolt::message_parameters::RunParameters;
use super::io::PooledBolt;
use super::io::{AcquireConfig, Pool, UpdateRtArgs};
use super::record_stream::RecordStream;
use super::transaction::Transaction;
use super::{EagerResult, ReducedDriverConfig, RoutingControl};
use crate::transaction::InnerTransaction;
use crate::{Result, ValueSend};
use bookmarks::Bookmarks;
pub use config::SessionConfig;
use retry::RetryPolicy;

#[derive(Debug)]
pub struct Session<'driver, C: AsRef<SessionConfig>> {
    config: C,
    pool: &'driver Pool,
    driver_config: &'driver ReducedDriverConfig,
    resolved_db: Option<String>,
    last_bookmarks: Option<Vec<String>>,
}

impl<'driver, C: AsRef<SessionConfig>> Session<'driver, C> {
    pub(crate) fn new(
        config: C,
        pool: &'driver Pool,
        driver_config: &'driver ReducedDriverConfig,
    ) -> Self {
        Session {
            config,
            pool,
            driver_config,
            resolved_db: None,
            last_bookmarks: None,
        }
    }

    pub fn auto_commit<'session, Q: AsRef<str>>(
        &'session mut self,
        query: Q,
    ) -> AutoCommitBuilder<
        'driver,
        'session,
        C,
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
        builder: AutoCommitBuilder<'driver, 'session, C, Q, KP, P, KM, M, FRes>,
    ) -> Result<R> {
        let cx = self.acquire_connection(builder.mode)?;
        let mut record_stream =
            RecordStream::new(Rc::new(RefCell::new(cx)), self.fetch_size(), true, None);
        let res = record_stream
            .run(RunParameters::new_auto_commit_run(
                builder.query.as_ref(),
                Some(builder.param.borrow()),
                self.last_raw_bookmarks().as_deref(),
                builder.timeout,
                Some(builder.meta.borrow()),
                builder.mode.as_protocol_str(),
                self.resolved_db().as_deref(),
                self.config.as_ref().impersonated_user.as_deref(),
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
            self.last_bookmarks = Some(vec![bookmark])
        }
        res
    }

    pub fn transaction<'session>(
        &'session mut self,
    ) -> TransactionBuilder<'driver, 'session, C, DefaultMetaKey, DefaultMeta> {
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
        builder: &TransactionBuilder<'driver, 'session, C, KM, M>,
        receiver: FTx,
    ) -> Result<R> {
        let connection = self.acquire_connection(builder.mode)?;
        let mut tx = InnerTransaction::new(connection, self.fetch_size());
        tx.begin(
            self.last_raw_bookmarks().as_deref(),
            builder.timeout,
            builder.meta.borrow(),
            builder.mode.as_protocol_str(),
            self.resolved_db().as_deref(),
            self.config.as_ref().impersonated_user.as_deref(),
        )?;
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
            self.last_bookmarks = Some(vec![bookmark])
        }
        res
    }

    fn resolve_db(&mut self) -> Result<()> {
        if self.resolved_db().is_none() && self.pool.is_routing() {
            debug!("Resolving home db");
            self.resolved_db = self.pool.resolve_home_db(UpdateRtArgs {
                db: &None,
                bookmarks: self.last_raw_bookmarks(),
                imp_user: &self.config.as_ref().impersonated_user,
            })?;
            debug!("Resolved home db to {:?}", &self.resolved_db);
        }
        Ok(())
    }

    #[inline]
    fn resolved_db(&self) -> &Option<String> {
        match self.resolved_db {
            None => &self.config.as_ref().database,
            Some(_) => &self.resolved_db,
        }
    }

    fn acquire_connection(&mut self, mode: RoutingControl) -> Result<PooledBolt<'driver>> {
        self.resolve_db()?;
        self.pool.acquire(AcquireConfig {
            mode,
            update_rt_args: UpdateRtArgs {
                db: self.resolved_db(),
                bookmarks: self.last_raw_bookmarks(),
                imp_user: &self.config.as_ref().impersonated_user,
            },
        })
    }

    #[inline]
    fn last_raw_bookmarks(&self) -> &Option<Vec<String>> {
        if self.last_bookmarks.is_some() {
            &self.last_bookmarks
        } else {
            &self.config.as_ref().bookmarks
        }
    }

    #[inline]
    pub fn last_bookmarks(&self) -> Bookmarks {
        Bookmarks::from_raw(self.last_raw_bookmarks().clone().unwrap_or_default())
    }

    #[inline]
    fn fetch_size(&self) -> i64 {
        self.config
            .as_ref()
            .fetch_size
            .unwrap_or(self.driver_config.fetch_size)
    }
}

pub struct AutoCommitBuilder<'driver, 'session, C: AsRef<SessionConfig>, Q, KP, P, KM, M, FRes> {
    session: Option<&'session mut Session<'driver, C>>,
    query: Q,
    _kp: PhantomData<KP>,
    param: P,
    _km: PhantomData<KM>,
    meta: M,
    timeout: Option<i64>,
    mode: RoutingControl,
    receiver: FRes,
}

fn default_receiver(res: &mut RecordStream) -> Result<EagerResult> {
    let keys = res.keys();
    let records = res.collect::<Result<_>>()?;
    let summary = res.consume().map(|summary| {
        summary.expect("first call and only applied on successful consume => summary must be Some")
    })?;
    Ok(EagerResult {
        keys,
        records,
        summary,
    })
}

type DefaultReceiver = fn(&mut RecordStream) -> Result<EagerResult>;
type DefaultParamKey = String;
type DefaultParam = HashMap<DefaultParamKey, ValueSend>;
type DefaultMetaKey = String;
type DefaultMeta = HashMap<DefaultMetaKey, ValueSend>;

impl<'driver, 'session, C: AsRef<SessionConfig>, Q: AsRef<str>>
    AutoCommitBuilder<
        'driver,
        'session,
        C,
        Q,
        DefaultParamKey,
        DefaultParam,
        DefaultMetaKey,
        DefaultMeta,
        DefaultReceiver,
    >
{
    fn new(session: &'session mut Session<'driver, C>, query: Q) -> Self {
        Self {
            session: Some(session),
            query,
            _kp: PhantomData,
            param: Default::default(),
            _km: PhantomData,
            meta: Default::default(),
            timeout: None,
            mode: RoutingControl::Write,
            receiver: default_receiver,
        }
    }
}

impl<
        'driver,
        'session,
        C: AsRef<SessionConfig>,
        Q: AsRef<str>,
        KP: Borrow<str> + Debug,
        P: Borrow<HashMap<KP, ValueSend>>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
        R,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
    > AutoCommitBuilder<'driver, 'session, C, Q, KP, P, KM, M, FRes>
{
    pub fn with_parameters<KP_: Borrow<str> + Debug, P_: Borrow<HashMap<KP_, ValueSend>>>(
        self,
        param: P_,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, KP_, P_, KM, M, FRes> {
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

    pub fn without_parameters(
        self,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, DefaultParamKey, DefaultParam, KM, M, FRes>
    {
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

    pub fn with_tx_meta<KM_: Borrow<str> + Debug, M_: Borrow<HashMap<KM_, ValueSend>>>(
        self,
        meta: M_,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, KP, P, KM_, M_, FRes> {
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

    pub fn without_tx_meta(
        self,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, KP, P, DefaultMetaKey, DefaultMeta, FRes> {
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

    pub fn with_tx_timeout(
        mut self,
        timeout: i64,
    ) -> result::Result<Self, ConfigureTimeoutError<Self>> {
        if timeout <= 0 {
            return Err(ConfigureTimeoutError::new(self));
        }
        self.timeout = Some(timeout);
        Ok(self)
    }

    pub fn without_tx_timeout(mut self) -> Self {
        self.timeout = Some(0);
        self
    }

    pub fn with_default_tx_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    pub fn with_routing_control(mut self, mode: RoutingControl) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_receiver<R_, FRes_: FnOnce(&mut RecordStream) -> Result<R_>>(
        self,
        receiver: FRes_,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, KP, P, KM, M, FRes_> {
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

    pub fn run(mut self) -> Result<R> {
        let session = self.session.take().unwrap();
        session.auto_commit_run(self)
    }
}

impl<'driver, 'session, C: AsRef<SessionConfig>, Q: Debug, KP, P: Debug, KM, M: Debug, FRes> Debug
    for AutoCommitBuilder<'driver, 'session, C, Q, KP, P, KM, M, FRes>
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
            .field("query", &self.query)
            .field("param", &self.param)
            .field("meta", &self.meta)
            .field("timeout", &self.timeout)
            .field("mode", &self.mode)
            .field("receiver", &"...")
            .finish()
    }
}

pub struct TransactionBuilder<'driver, 'session, C: AsRef<SessionConfig>, KM, M> {
    session: Option<&'session mut Session<'driver, C>>,
    _km: PhantomData<KM>,
    meta: M,
    timeout: Option<i64>,
    mode: RoutingControl,
}

impl<'driver, 'session, C: AsRef<SessionConfig>>
    TransactionBuilder<'driver, 'session, C, DefaultMetaKey, DefaultMeta>
{
    fn new(session: &'session mut Session<'driver, C>) -> Self {
        Self {
            session: Some(session),
            _km: PhantomData,
            meta: Default::default(),
            timeout: None,
            mode: RoutingControl::Write,
        }
    }
}

impl<
        'driver,
        'session,
        C: AsRef<SessionConfig>,
        KM: Borrow<str> + Debug,
        M: Borrow<HashMap<KM, ValueSend>>,
    > TransactionBuilder<'driver, 'session, C, KM, M>
{
    pub fn with_tx_meta<KM_: Borrow<str> + Debug, M_: Borrow<HashMap<KM_, ValueSend>>>(
        self,
        meta: M_,
    ) -> TransactionBuilder<'driver, 'session, C, KM_, M_> {
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

    pub fn without_tx_meta(
        self,
    ) -> TransactionBuilder<'driver, 'session, C, DefaultMetaKey, DefaultMeta> {
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

    pub fn with_tx_timeout(
        mut self,
        timeout: i64,
    ) -> result::Result<Self, ConfigureTimeoutError<Self>> {
        if timeout <= 0 {
            return Err(ConfigureTimeoutError::new(self));
        }
        self.timeout = Some(timeout);
        Ok(self)
    }

    pub fn without_tx_timeout(mut self) -> Self {
        self.timeout = Some(0);
        self
    }

    pub fn with_default_tx_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    pub fn with_routing_control(mut self, mode: RoutingControl) -> Self {
        self.mode = mode;
        self
    }

    pub fn run<R>(mut self, receiver: impl FnOnce(Transaction) -> Result<R>) -> Result<R> {
        let session = self.session.take().unwrap();
        session.transaction_run(&self, receiver)
    }

    pub fn run_with_retry<R, P: RetryPolicy>(
        mut self,
        retry_policy: P,
        mut receiver: impl FnMut(Transaction) -> Result<R>,
    ) -> result::Result<R, P::Error> {
        let session = self.session.take().unwrap();
        retry_policy.execute(|| session.transaction_run(&self, &mut receiver))
    }
}

#[derive(Debug, Error)]
#[error("timeout must be > 0")]
pub struct ConfigureTimeoutError<Builder> {
    pub builder: Builder,
}

impl<Builder> ConfigureTimeoutError<Builder> {
    fn new(builder: Builder) -> Self {
        Self { builder }
    }
}
