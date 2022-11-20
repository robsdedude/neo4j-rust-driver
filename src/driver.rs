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

mod config;
mod session;
pub use config::{ConnectionConfig, DriverConfig};
pub use session::{Session, SessionConfig};

#[derive(Debug)]
pub struct Driver {
    pub(crate) config: DriverConfig,
    pub(crate) connection_config: ConnectionConfig,
}

impl Driver {
    pub fn new(connection_config: ConnectionConfig, config: DriverConfig) -> Self {
        Driver {
            config,
            connection_config,
        }
    }

    pub fn session<'a>(&self, config: &'a SessionConfig) -> Session<'a> {
        Session { config }
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
