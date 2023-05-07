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

use log::debug;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;

use super::io::bolt::PackStreamSerialize;
use super::io::{AcquireConfig, Pool, UpdateRtArgs};
use super::record_stream::RecordStream;
use crate::driver::RoutingControl;
use crate::summary::Summary;
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
    ) -> AutoCommitTransaction<'driver, 'session, C, Q, DefaultMeta, DefaultReceiver> {
        AutoCommitTransaction::new(self, query)
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
        builder: AutoCommitTransaction<'driver, 'session, C, Q, M, FRes>,
        parameters: P,
    ) -> Result<R> {
        self.resolve_db()?;
        let cx = self.pool.acquire(AcquireConfig {
            mode: builder.mode,
            update_rt_args: UpdateRtArgs {
                db: self.resolved_db(),
                bookmarks: self.last_raw_bookmarks(),
                imp_user: &self.config.as_ref().impersonated_user,
            },
        })?;
        let mut run_prep = cx.run_prepare(
            builder.query.as_ref(),
            self.last_raw_bookmarks().as_deref(),
            match builder.mode {
                RoutingControl::Read => Some("r"),
                RoutingControl::Write => None,
            },
            self.resolved_db().as_deref(),
            None,
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
        let bookmarks = record_stream.into_bookmark();
        self.last_bookmarks = bookmarks.map(|bm| vec![bm]);
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

pub struct AutoCommitTransaction<'driver, 'session, C, Q, M, FRes> {
    session: Option<&'session mut Session<'driver, C>>,
    query: Q,
    meta: M,
    timeout: Option<i64>,
    mode: RoutingControl,
    receiver: FRes,
}

fn default_receiver(res: &mut RecordStream) -> Result<Summary> {
    res.consume().map(|summary| {
        summary.expect("first call and only applied on successful consume => summary must be Some")
    })
}

type DefaultReceiver = fn(&mut RecordStream) -> Result<Summary>;
type DefaultMeta = HashMap<String, ValueSend>;

impl<'driver, 'session, C: AsRef<SessionConfig>, Q: AsRef<str>>
    AutoCommitTransaction<'driver, 'session, C, Q, DefaultMeta, DefaultReceiver>
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
    > AutoCommitTransaction<'driver, 'session, C, Q, M, FRes>
{
    pub fn with_tx_meta<M_: Borrow<HashMap<String, ValueSend>>>(
        self,
        meta: M_,
    ) -> AutoCommitTransaction<'driver, 'session, C, Q, M_, FRes> {
        let Self {
            session,
            query,
            meta: _,
            timeout,
            mode,
            receiver,
        } = self;
        AutoCommitTransaction {
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
    ) -> AutoCommitTransaction<'driver, 'session, C, Q, M, FRes_> {
        let Self {
            session,
            query,
            meta,
            timeout,
            mode,
            receiver: _,
        } = self;
        AutoCommitTransaction {
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
    > Debug for AutoCommitTransaction<'driver, 'session, C, Q, M, FRes>
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
