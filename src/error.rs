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

use crate::driver::io::bolt::BoltMeta;
use crate::ValueReceive;

#[derive(Error, Debug)]
// #[non_exhaustive]
pub enum Neo4jError {
    /// used when
    ///  * Experiencing a connectivity error.  
    ///    E.g., not able to connect, a broken socket,
    ///    not able to fetch routing information
    #[error("connection failed: {message} (during commit: {during_commit})")]
    // #[non_exhaustive]
    Disconnect {
        message: String,
        // #[backtrace]
        // #[from]
        // #[source]
        source: Option<io::Error>,
        /// Will be true when connection was lost while the driver cannot be
        /// sure whether the ongoing transaction has been committed or not.
        /// To recover from this situation, business logic is required to check
        /// whether the transaction should or shouldn't be retried.
        during_commit: bool,
    },
    /// used when
    ///  * Trying to send an unsupported parameter.  
    ///    e.g., a too large Vec (max. `u32::MAX` elements).
    ///  * Connecting with an incompatible server/using a feature that's not
    ///    supported over the negotiated protocol version.
    ///  * Non-IO error occurs during serialization (e.g., trying to serialize
    ///    `Value::BrokenValue`)
    ///  * Can be generated using `GetSingleRecordError::into()`
    ///    (the driver itself won't perform this conversion)
    #[error("invalid configuration: {message}")]
    // #[non_exhaustive]
    InvalidConfig {
        message: String,
        // backtrace: Backtrace,
    },
    /// used when
    ///  * the server returns an error
    #[error("{error}")]
    // #[non_exhaustive]
    ServerError { error: ServerError },
    #[error(
        "the driver encountered a protocol violation, \
        this is likely a bug in the driver or the server: {message}"
    )]
    // #[non_exhaustive]
    ProtocolError {
        message: String,
        // backtrace: Backtrace
    },
}

impl Neo4jError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Neo4jError::ServerError { error } => error.is_retryable(),
            Neo4jError::Disconnect { during_commit, .. } => !during_commit,
            _ => false,
        }
    }

    pub(crate) fn read_err(err: io::Error) -> Self {
        Self::Disconnect {
            message: format!("failed to read: {}", err),
            source: Some(err),
            during_commit: false,
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
            during_commit: false,
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
            during_commit: false,
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
            during_commit: false,
        }
    }

    pub(crate) fn protocol_error<S: Into<String>>(message: S) -> Self {
        Self::ProtocolError {
            message: message.into(),
        }
    }

    pub(crate) fn failed_commit(mut self) -> Self {
        if let Self::Disconnect { during_commit, .. } = &mut self {
            *during_commit = true;
        }
        self
    }

    pub(crate) fn wrap_commit<T>(res: Result<T>) -> Result<T> {
        match res {
            Ok(t) => Ok(t),
            Err(err) => Err(err.failed_commit()),
        }
    }

    pub(crate) fn fatal_during_discovery(&self) -> bool {
        match self {
            Neo4jError::InvalidConfig { .. } => true,
            Neo4jError::ServerError { error } => error.fatal_during_discovery(),
            _ => false,
        }
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
            Some(ValueReceive::String(code)) => code,
            _ => "Neo.DatabaseError.General.UnknownError".into(),
        };
        let message = match meta.remove("message") {
            Some(ValueReceive::String(message)) => message,
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
        match self.code() {
            "Neo.TransientError.Transaction.Terminated"
            | "Neo.TransientError.Transaction.LockClientStopped" => {
                // In 5.0, these errors have been re-classified as ClientError.
                // For backwards compatibility with Neo4j 4.4 and earlier, we re-map
                // them in the driver, too.
                "ClientError"
            }
            _ => self.code.split('.').nth(1).unwrap_or(""),
        }
    }

    pub fn category(&self) -> &str {
        self.code.split('.').nth(2).unwrap_or("")
    }

    pub fn title(&self) -> &str {
        self.code.split('.').nth(3).unwrap_or("")
    }

    fn is_retryable(&self) -> bool {
        self.code() == "Neo.ClientError.Security.AuthorizationExpired"
            || self.classification() == "TransientError"
    }

    fn fatal_during_discovery(&self) -> bool {
        // TODO: add the other exceptions
        match self.code() {
            "Neo.ClientError.Database.DatabaseNotFound"
            | "Neo.ClientError.Transaction.InvalidBookmark"
            | "Neo.ClientError.Transaction.InvalidBookmarkMixture"
            | "Neo.ClientError.Statement.TypeError"
            | "Neo.ClientError.Statement.ArgumentError"
            | "Neo.ClientError.Request.Invalid" => true,
            code => {
                code.starts_with("Neo.ClientError.Security.")
                    && code != "Neo.ClientError.Security.AuthorizationExpired"
            }
        }
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
        Neo4jError::ServerError { error: err }
    }
}
