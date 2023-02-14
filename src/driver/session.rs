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

mod bookmarks;
mod config;

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::ops::Deref;
use std::sync::Arc;

use super::io::{bolt::TcpBolt, Pool};
use crate::{PackStreamSerialize, RecordStream, Result, Value};
pub use bookmarks::Bookmarks;
pub use config::SessionConfig;

#[derive(Debug)]
pub struct Session<'a> {
    config: &'a SessionConfig,
    pool: &'a Pool,
    connection: Option<TcpBolt>,
    // run_record_stream: Option<RecordStream>,
}

#[derive(Debug, Clone)]
pub struct SessionRunConfig<
    K1: Deref<Target = str> + Debug,
    S1: PackStreamSerialize,
    K2: Deref<Target = str> + Debug,
    S2: PackStreamSerialize,
> {
    parameters: Option<HashMap<K1, S1>>,
    tx_meta: Option<HashMap<K2, S2>>,
}

impl<'a> Session<'a> {
    pub(crate) fn new(config: &'a SessionConfig, pool: &'a Pool) -> Self {
        Session {
            config,
            pool,
            connection: None,
            // run_record_stream: None,
        }
    }

    pub fn run<
        F: Fn(&mut RecordStream) -> Result<()>,
        K1: Deref<Target = str> + Debug,
        S1: PackStreamSerialize,
        K2: Deref<Target = str> + Debug,
        S2: PackStreamSerialize,
    >(
        &mut self,
        receiver: F,
        query: &str,
        config: &SessionRunConfig<K1, S1, K2, S2>,
    ) -> Result<()> {
        self.connect()?;
        let cx = self.connection.as_mut().unwrap();
        // cx.run(
        //     query: &str,
        //     parameters: Option<&HashMap<String, Value>>,
        //     bookmarks: Option<&[String]>,
        //     tx_timeout: Option<i64>,
        //     tx_meta: Option<&HashMap<String, Value>>,
        //     mode: Option<&str>,
        //     db: Option<&str>,
        //     imp_user: Option<&str>,
        // )?;

        let mut record_stream = RecordStream::new(cx);
        record_stream.run(
            query,
            config.parameters.as_ref(),
            self.config.bookmarks.as_deref(),
            None,
            config.tx_meta.as_ref(),
            None,
            self.config.database.as_deref(),
            None,
        )?;
        let result = receiver(&mut record_stream);
        Ok(())
    }

    fn connect(&mut self) -> Result<()> {
        if self.connection.is_some() {
            self.disconnect()?;
        }
        self.connection = Some(self.pool.acquire()?);
        Ok(())
    }

    fn disconnect(&mut self) -> Result<()> {
        self.connection = None;
        Ok(())
    }
}
