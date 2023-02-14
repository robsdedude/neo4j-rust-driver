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

use super::bolt::{self, TcpBolt};
use crate::{Address, Result, Value};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct PoolConfig {
    pub(crate) address: Address,
    // pub(crate) routing: bool,
    pub(crate) routing_context: Option<HashMap<String, Value>>,
    // pub(crate) ssl_context: Option<SslContext>,
    pub(crate) user_agent: String,
    pub(crate) auth: HashMap<String, Value>,
}

#[derive(Debug)]
pub struct Pool {
    config: PoolConfig,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        Pool { config }
    }

    pub(crate) fn acquire(&self) -> Result<TcpBolt> {
        // TODO: this pool needs some... pooling
        let mut connection = bolt::open(&self.config.address)?;
        connection.hello(
            self.config.user_agent.as_str(),
            &self.config.auth,
            self.config.routing_context.as_ref(),
        )?;
        connection.write_all()?;
        connection.read_all()?;
        Ok(connection)
    }
}
