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

use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::ops::{Deref, DerefMut};

use super::io::{bolt::RunPreparation, AcquireConfig, Pool, PooledBolt};
use crate::{PackStreamSerialize, RecordStream, Result, RoutingControl};
pub use bookmarks::Bookmarks;
pub use config::SessionConfig;

#[derive(Debug)]
pub struct Session<'a, C> {
    config: C,
    pool: &'a Pool,
    resolved_db: Option<String>,
}

impl<'a, C: AsRef<SessionConfig>> Session<'a, C> {
    pub(crate) fn new(config: C, pool: &'a Pool) -> Self {
        Session {
            config,
            pool,
            resolved_db: None,
        }
    }

    pub fn run_with_config<
        FConf: FnOnce(&mut SessionRunConfig) -> Result<()>,
        FRes: FnOnce(&mut RecordStream) -> Result<R>,
        R,
    >(
        &mut self,
        query: impl AsRef<str>,
        config_cb: FConf,
        receiver: FRes,
    ) -> Result<R> {
        let mut cx = self.pool.acquire(AcquireConfig {
            db: self.resolved_db()?,
            mode: RoutingControl::Write,
        })?;
        let run_prep = cx.run_prepare(
            query.as_ref(),
            self.config.as_ref().bookmarks.as_deref(),
            None,
            self.config.as_ref().database.as_deref(),
            None,
        )?;
        let mut conf = SessionRunConfig::new(run_prep);
        config_cb(&mut conf)?;
        let run_prep = conf.into_run_prep();
        let mut record_stream = RecordStream::new(&mut cx);
        let res = record_stream
            .run(run_prep)
            .and_then(|_| receiver(&mut record_stream));
        match res {
            Ok(r) => {
                record_stream.consume()?;
                Ok(r)
            }
            Err(e) => {
                let _ = record_stream.consume();
                Err(e)
            }
        }
    }

    pub fn run<FRes: FnOnce(&mut RecordStream) -> Result<R>, R>(
        &mut self,
        query: impl AsRef<str>,
        receiver: FRes,
    ) -> Result<R> {
        self.run_with_config(query, |_| Ok(()), receiver)
    }

    fn resolved_db(&mut self) -> Result<&Option<String>> {
        if self.resolved_db.is_none()
            && self.config.as_ref().database.is_none()
            && self.pool.is_routing()
        {
            self.resolved_db = self.pool.resolve_home_db()?;
        }
        Ok(&self.resolved_db)
    }
}

#[derive(Debug)]
pub struct SessionRunConfig {
    run_prep: RunPreparation,
}

impl SessionRunConfig {
    fn new(run_prep: RunPreparation) -> Self {
        Self { run_prep }
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

    fn into_run_prep(self) -> RunPreparation {
        self.run_prep
    }
}
