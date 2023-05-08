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

use log::{debug, info};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

use super::io::bolt::PackStreamSerialize;
use super::io::{AcquireConfig, Pool, UpdateRtArgs};
use super::record_stream::RecordStream;
use super::transaction::Transaction;
use crate::driver::io::PooledBolt;
use crate::driver::{EagerResult,, RoutingControl};
use crate::transaction::InnerTransaction;
use crate::{Result, ValueSend};
use bookmarks::Bookmarks;
pub use config::SessionConfig;

#[derive(Debug)]
pub struct Session<'driver, C> {
    config: C,
    pool: &'driver Pool,
    resolved_db: Option<String>,
    last_bookmarks: Option<Vec<String>>,
}

impl<'driver, C: AsRef<SessionConfig>> Session<'driver, C> {
    pub(crate) fn new(config: C, pool: &'driver Pool) -> Self {
        Session {
            config,
            pool,
            resolved_db: None,
            last_bookmarks: None,
        }
    }

    pub fn auto_commit<'session, Q: AsRef<str>>(
        &'session mut self,
        query: Q,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, DefaultMeta, DefaultReceiver> {
        AutoCommitBuilder::new(self, query)
    }

    fn auto_commit_run<
        'session,
        Q: AsRef<str>,
        M: Borrow<HashMap<String, ValueSend>>,
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
        R,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
    >(
        &'session mut self,
        builder: AutoCommitBuilder<'driver, 'session, C, Q, M, FRes>,
        parameters: P,
    ) -> Result<R> {
        let cx = self.acquire_connection(builder.mode)?;
        let mut run_prep = cx.run_prepare(
            builder.query.as_ref(),
            self.last_raw_bookmarks().as_deref(),
            builder.mode.as_protocol_str(),
            self.resolved_db().as_deref(),
            self.config.as_ref().impersonated_user.as_deref(),
        )?;
        if !builder.meta.borrow().is_empty() {
            run_prep.with_tx_meta(builder.meta.borrow())?;
        }
        if !parameters.borrow().is_empty() {
            run_prep.with_parameters(parameters.borrow())?;
        }
        if let Some(timeout) = builder.timeout {
            run_prep.with_tx_timeout(timeout)?;
        }
        let mut record_stream = RecordStream::new(Rc::new(RefCell::new(cx)), true);
        let res = record_stream
            .run(run_prep)
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
    ) -> TransactionBuilder<'driver, 'session, C, DefaultMeta> {
        TransactionBuilder::new(self)
    }

    fn transaction_run<
        'session,
        M: Borrow<HashMap<String, ValueSend>>,
        R,
        FTx: for<'tx> FnOnce(Transaction<'driver, 'tx>) -> Result<R>,
    >(
        &'session mut self,
        builder: TransactionBuilder<'driver, 'session, C, M>,
        receiver: FTx,
    ) -> Result<R> {
        let connection = self.acquire_connection(builder.mode)?;
        let mut tx = InnerTransaction::new(connection);
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
}

pub struct AutoCommitBuilder<'driver, 'session, C, Q, M, FRes> {
    session: Option<&'session mut Session<'driver, C>>,
    query: Q,
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
type DefaultMeta = HashMap<String, ValueSend>;

impl<'driver, 'session, C: AsRef<SessionConfig>, Q: AsRef<str>>
    AutoCommitBuilder<'driver, 'session, C, Q, DefaultMeta, DefaultReceiver>
{
    fn new(session: &'session mut Session<'driver, C>, query: Q) -> Self {
        Self {
            session: Some(session),
            query,
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
        M: Borrow<HashMap<String, ValueSend>>,
        R,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
    > AutoCommitBuilder<'driver, 'session, C, Q, M, FRes>
{
    pub fn with_tx_meta<M_: Borrow<HashMap<String, ValueSend>>>(
        self,
        meta: M_,
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, M_, FRes> {
        let Self {
            session,
            query,
            meta: _,
            timeout,
            mode,
            receiver,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            meta,
            timeout,
            mode,
            receiver,
        }
    }

    pub fn with_tx_timeout(mut self, timeout: i64) -> Self {
        self.timeout = Some(timeout);
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
    ) -> AutoCommitBuilder<'driver, 'session, C, Q, M, FRes_> {
        let Self {
            session,
            query,
            meta,
            timeout,
            mode,
            receiver: _,
        } = self;
        AutoCommitBuilder {
            session,
            query,
            meta,
            timeout,
            mode,
            receiver,
        }
    }

    pub fn run(mut self) -> Result<R> {
        let session = self.session.take().unwrap();
        session.auto_commit_run(self, HashMap::<String, ValueSend>::new())
    }

    pub fn run_with_parameters<
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
    >(
        mut self,
        parameters: P,
    ) -> Result<R> {
        let session = self.session.take().unwrap();
        session.auto_commit_run(self, parameters)
    }
}

impl<
        'driver,
        'session,
        C: AsRef<SessionConfig> + Debug,
        Q: AsRef<str>,
        M: Borrow<HashMap<String, ValueSend>>,
        FRes,
    > Debug for AutoCommitBuilder<'driver, 'session, C, Q, M, FRes>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AutoCommitBuilder {{\n  session: {:?}\n  query: {:?}\n  \
            meta: {:?}\n  timeout: {:?}\n  mode: {:?}\n  receiver: ...\n}}",
            match self.session {
                None => "None",
                Some(_) => "Some(...)",
            },
            self.query.as_ref(),
            self.meta.borrow(),
            self.timeout,
            self.mode
        )
    }
}

pub struct TransactionBuilder<'driver, 'session, C, M> {
    session: Option<&'session mut Session<'driver, C>>,
    meta: M,
    timeout: Option<i64>,
    mode: RoutingControl,
}

impl<'driver, 'session, C: AsRef<SessionConfig>>
    TransactionBuilder<'driver, 'session, C, DefaultMeta>
{
    fn new(session: &'session mut Session<'driver, C>) -> Self {
        Self {
            session: Some(session),
            meta: Default::default(),
            timeout: None,
            mode: RoutingControl::Write,
        }
    }
}

impl<'driver, 'session, C: AsRef<SessionConfig>, M: Borrow<HashMap<String, ValueSend>>>
    TransactionBuilder<'driver, 'session, C, M>
{
    pub fn with_tx_meta<M_: Borrow<HashMap<String, ValueSend>>>(
        self,
        meta: M_,
    ) -> TransactionBuilder<'driver, 'session, C, M_> {
        let Self {
            session,
            meta: _,
            timeout,
            mode,
        } = self;
        TransactionBuilder {
            session,
            meta,
            timeout,
            mode,
        }
    }

    pub fn with_tx_timeout(mut self, timeout: i64) -> Self {
        self.timeout = Some(timeout);
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

    pub fn run<R, FTx: FnOnce(Transaction) -> Result<R>>(mut self, receiver: FTx) -> Result<R> {
        let session = self.session.take().unwrap();
        session.transaction_run(self, receiver)
    }
}
