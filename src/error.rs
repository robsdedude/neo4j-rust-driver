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
use std::io::Error;
use thiserror::Error;

use crate::driver::io::bolt::BoltMeta;
use crate::Value;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Neo4jError {
    /// used when
    ///  * Experiencing a connectivity error.  
    ///    E.g., not able to connect, a broken socket,
    ///    not able to fetch routing information
    #[error("connection failed: {message}")]
    Disconnect {
        // #[backtrace]
        // #[from]
        message: String,
        // #[source]
        source: Option<io::Error>,
    },
    /// used when
    ///  * Trying to send an unsupported parameter.  
    ///    e.g., a too large Vec (max. `u32::MAX` elements).
    ///  * Connecting with an incompatible server/using a feature that's not
    ///    supported over the negotiated protocol version.
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
    ProtocolError {
        message: String,
        // backtrace: Backtrace
    },
}

impl Neo4jError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Neo4jError::ServerError(err) => err.is_retryable(),
            Neo4jError::Disconnect { .. } => true,
            _ => false,
        }
    }

    pub(crate) fn read_err(err: io::Error) -> Self {
        Self::Disconnect {
            message: format!("failed to read: {}", err),
            source: Some(err),
        }
    }

    pub(crate) fn wrap_read<T>(res: io::Result<T>) -> Result<T> {
        match res {
            Ok(t) => Ok(t),
            Err(err) => Err(Self::read_err(err)),
        }
    }

    pub(crate) fn write_error(err: io::Error) -> Neo4jError {
        Self::Disconnect {
            message: format!("failed to write: {}", err),
            source: Some(err),
        }
    }

    pub(crate) fn wrap_write<T>(res: io::Result<T>) -> Result<T> {
        match res {
            Ok(t) => Ok(t),
            Err(err) => Err(Self::write_error(err)),
        }
    }

    pub(crate) fn connect_error(err: io::Error) -> Neo4jError {
        Self::Disconnect {
            message: format!("failed to open connection: {}", err),
            source: Some(err),
        }
    }

    pub(crate) fn wrap_connect<T>(res: io::Result<T>) -> Result<T> {
        match res {
            Ok(t) => Ok(t),
            Err(err) => Err(Self::connect_error(err)),
        }
    }

    pub(crate) fn disconnect<S: Into<String>>(message: S) -> Self {
        Self::Disconnect {
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn fatal_during_discovery(&self) -> bool {
        // TODO: implement real logic
        false
    }
}

#[derive(Debug)]
pub struct ServerError {
    code: String,
    message: String,
}

impl ServerError {
    pub fn new(code: String, message: String) -> Self {
        Self { code, message }
    }

    pub fn from_meta(mut meta: BoltMeta) -> Self {
        let code = match meta.remove("code") {
            Some(Value::String(code)) => code,
            _ => "Neo.DatabaseError.General.UnknownError".into(),
        };
        let message = match meta.remove("message") {
            Some(Value::String(message)) => message,
            _ => "An unknown error occurred.".into(),
        };
        Self { code, message }
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

    fn is_retryable(&self) -> bool {
        todo!()
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error {}: {}", self.code, self.message)
    }
}

pub type Result<T> = std::result::Result<T, Neo4jError>;

impl From<ServerError> for Neo4jError {
    fn from(err: ServerError) -> Self {
        Neo4jError::ServerError(err)
    }
}
