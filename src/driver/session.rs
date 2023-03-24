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

pub mod bookmarks;
pub(crate) mod config;

use log::debug;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;

use super::io::bolt::PackStreamSerialize;
use super::io::{bolt::RunPreparation, AcquireConfig, Pool, UpdateRtArgs};
use super::record_stream::RecordStream;
use crate::driver::RoutingControl;
use crate::Result;
use bookmarks::Bookmarks;
pub use config::SessionConfig;

#[derive(Debug)]
pub struct Session<'a, C> {
    config: C,
    pool: &'a Pool,
    resolved_db: Option<String>,
    latest_bookmarks: Option<Vec<String>>,
}

impl<'a, C: AsRef<SessionConfig>> Session<'a, C> {
    pub(crate) fn new(config: C, pool: &'a Pool) -> Self {
        Session {
            config,
            pool,
            resolved_db: None,
            latest_bookmarks: None,
        }
    }

    pub fn auto_commit_with_extra<
        FConf: FnOnce(&mut AutoCommitExtra) -> Result<()>,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
        R,
    >(
        &mut self,
        query: impl AsRef<str>,
        extra_cb: FConf,
        receiver: FRes,
    ) -> Result<R> {
        self.resolve_db()?;
        let mut cx = self.pool.acquire(AcquireConfig {
            mode: self.config.as_ref().run_routing,
            update_rt_args: UpdateRtArgs {
                db: &self.resolved_db,
                bookmarks: self.latest_raw_bookmarks(),
                imp_user: &self.config.as_ref().impersonated_user,
            },
        })?;
        let run_prep = cx.run_prepare(
            query.as_ref(),
            self.latest_raw_bookmarks().as_deref(),
            None,
            self.config.as_ref().database.as_deref(),
            None,
        )?;
        let mut conf = AutoCommitExtra::new(run_prep);
        extra_cb(&mut conf)?;
        let run_prep = conf.into_run_prep();
        let mut record_stream = RecordStream::new(&mut cx, true);
        let res = record_stream
            .run(run_prep)
            .and_then(|_| receiver(&mut record_stream));
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
        self.latest_bookmarks = bookmarks.map(|bm| vec![bm]);
        res
    }

    pub fn auto_commit<FRes: FnOnce(&mut RecordStream) -> Result<R>, R>(
        &mut self,
        query: impl AsRef<str>,
        receiver: FRes,
    ) -> Result<R> {
        self.auto_commit_with_extra(query, |_| Ok(()), receiver)
    }

    fn resolve_db(&mut self) -> Result<()> {
        if self.resolved_db().is_none() && self.pool.is_routing() {
            debug!("Resolving home db");
            self.resolved_db = self.pool.resolve_home_db(UpdateRtArgs {
                db: &None,
                bookmarks: self.latest_raw_bookmarks(),
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
    fn latest_raw_bookmarks(&self) -> &Option<Vec<String>> {
        if self.latest_bookmarks.is_some() {
            &self.latest_bookmarks
        } else {
            &self.config.as_ref().bookmarks
        }
    }

    #[inline]
    pub fn latest_bookmarks(&self) -> Bookmarks {
        Bookmarks::from_raw(self.latest_raw_bookmarks().clone().unwrap_or_default())
    }
}

#[derive(Debug)]
pub struct AutoCommitExtra {
    run_prep: RunPreparation,
    mode: RoutingControl,
}

impl AutoCommitExtra {
    fn new(run_prep: RunPreparation) -> Self {
        Self {
            run_prep,
            mode: RoutingControl::Write,
        }
    }

    pub fn with_parameters<
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
    >(
        &mut self,
        parameters: P,
    ) -> Result<()> {
        self.run_prep.with_parameters(parameters.borrow())?;
        Ok(())
    }

    pub fn with_transaction_meta<
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
    >(
        &mut self,
        tx_meta: P,
    ) -> Result<()> {
        self.run_prep.with_tx_meta(tx_meta.borrow())?;
        Ok(())
    }

    pub fn with_transaction_timeout(&mut self, timeout: i64) -> Result<()> {
        self.run_prep.with_tx_timeout(timeout)?;
        Ok(())
    }

    pub fn with_mode(&mut self, mode: RoutingControl) {
        self.mode = mode;
    }

    fn into_run_prep(self) -> RunPreparation {
        self.run_prep
    }
}
