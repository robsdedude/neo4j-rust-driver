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

use std::sync::Arc;
use std::time::Duration;

use crate::Result;
use config::auth::AuthToken;
pub use config::{
    ConfigureFetchSizeError, ConnectionConfig, ConnectionConfigParseError, DriverConfig,
    TlsConfigError,
};
pub use eager_result::EagerResult;
use io::{AcquireConfig, Pool, PoolConfig, PooledBolt, SessionAuth, UpdateRtArgs};
pub use record::Record;
use session::config::InternalSessionConfig;
use session::{Session, SessionConfig};
use summary::ServerInfo;

pub mod auth {
    pub use super::config::auth::*;
}

#[derive(Debug)]
pub struct Driver {
    pub(crate) config: ReducedDriverConfig,
    pub(crate) pool: Pool,
    capability_check_config: SessionConfig,
}

impl Driver {
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
            capability_check_config: SessionConfig::default().with_database(String::from("system")),
        }
    }

    pub fn session<C: AsRef<SessionConfig>>(&self, config: C) -> Session<C> {
        let config = InternalSessionConfig {
            config,
            idle_time_before_connection_test: self.config.idle_time_before_connection_test,
        };
        Session::new(config, &self.pool, &self.config)
    }

    pub fn verify_connectivity(&self) -> Result<()> {
        self.acquire_connectivity_checked().map(|_| ())
    }

    pub fn get_server_info(&self) -> Result<ServerInfo> {
        self.acquire_connectivity_checked()
            .map(|connection| ServerInfo::new(&connection))
    }

    pub fn verify_authentication(&self, auth: Arc<AuthToken>) -> Result<bool> {
        self.session(&self.capability_check_config)
            .verify_authentication(&auth)
    }

    fn acquire_connectivity_checked(&self) -> Result<PooledBolt> {
        let config = InternalSessionConfig {
            config: SessionConfig::default(),
            idle_time_before_connection_test: Some(Duration::ZERO),
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
                db: &self.capability_check_config.database,
                bookmarks: &None,
                imp_user: &None,
                session_auth: SessionAuth::None,
                idle_time_before_connection_test: None,
            },
        })
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
