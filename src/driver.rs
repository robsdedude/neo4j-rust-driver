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

pub use config::{ConfigureFetchSizeError, ConnectionConfig, DriverConfig};
use std::sync::Arc;

pub use eager_result::EagerResult;
use io::{Pool, PoolConfig};
pub use record::Record;
use session::{Session, SessionConfig};

#[derive(Debug)]
pub struct Driver {
    pub(crate) config: ReducedDriverConfig,
    pub(crate) pool: Pool,
}

impl Driver {
    pub fn new(connection_config: ConnectionConfig, config: DriverConfig) -> Self {
        let pool_config = PoolConfig {
            routing_context: connection_config.routing_context,
            user_agent: config.user_agent,
            auth: config.auth,
            max_connection_pool_size: config.max_connection_pool_size,
            connection_timeout: config.connection_timeout,
            connection_acquisition_timeout: config.connection_acquisition_timeout,
        };
        Driver {
            config: ReducedDriverConfig {
                fetch_size: config.fetch_size,
            },
            pool: Pool::new(Arc::new(connection_config.address), pool_config),
        }
    }

    pub fn session<C: AsRef<SessionConfig>>(&self, config: C) -> Session<C> {
        Session::new(config, &self.pool, &self.config)
    }
}

#[derive(Debug)]
pub(crate) struct ReducedDriverConfig {
    pub(crate) fetch_size: i64,
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

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_session() {
    //     let driver = Driver::new(
    //         ConnectionConfig::new("localhost".into()),
    //         DriverConfig::new(),
    //     );
    //     let db = String::from("foo_bar");
    //     let session_config = Box::new(SessionConfig::new().with_database(db.as_str()));
    //     let session = driver.session(&session_config);
    //     dbg!(&session);
    // }
}
