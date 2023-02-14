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

// use std::backtrace::Backtrace;
use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Neo4jError {
    /// used when
    ///  * experiencing a socket error
    #[error("connection lost")]
    Disconnect {
        #[from]
        // #[backtrace]
        source: io::Error,
    },
    /// used when
    ///  * Trying to send an unsupported parameter.  
    ///    e.g., a too large Vec (max. `u32::MAX` elements).
    ///  * Connecting with an incompatible server.
    ///  * Non-IO error occurs during serialization (e.g., trying to serialize
    ///    `Value::BrokenValue`)
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        message: String,
        // backtrace: Backtrace,
    },
    /// used when
    ///  * the server returns an error
    #[error("{0}")]
    ServerError(ServerError),
    #[error(
        "the driver encountered a protocol violation, \
        this is likely a bug in the driver or the server: {message}"
    )]
    ProtocolError { message: String },
}

impl Neo4jError {
    pub fn is_retryable() {
        todo!();
    }
}

#[derive(Debug)]
pub struct ServerError {
    code: String,
    message: String,
}

impl ServerError {
    pub fn new(code: String, message: String) -> Self {
        ServerError { code, message }
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn classification(&self) -> &str {
        self.code.split('.').nth(1).unwrap_or("")
    }

    pub fn category(&self) -> &str {
        self.code.split('.').nth(2).unwrap_or("")
    }

    pub fn title(&self) -> &str {
        self.code.split('.').nth(3).unwrap_or("")
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error {}: {}", self.code, self.message)
    }
}

pub type Result<T> = std::result::Result<T, Neo4jError>;
