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

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use neo4j::driver::{ConfigureFetchSizeError, ConnectionConfigParseError, TlsConfigError};
use neo4j::error::{ServerError, UserCallbackError};
use neo4j::retry::RetryError;
use neo4j::Neo4jError;

use super::cypher_value::{BrokenValueError, NotADriverValueError};
use super::{BackendId, Generator};

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)] // names are reflecting TestKit protocol
pub(super) enum TestKitError {
    DriverError {
        error_type: String,
        msg: String,
        code: Option<String>,
        id: Option<BackendId>,
        retryable: bool,
    },
    FrontendError {
        msg: String,
    },
    BackendError {
        msg: String,
    },
    FatalError {
        #[allow(dead_code)] // used (and usefull) in Debug impl
        error: String,
    },
}

impl Display for TestKitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<Neo4jError> for TestKitError {
    fn from(err: Neo4jError) -> Self {
        let retryable = err.is_retryable();
        match err {
            Neo4jError::Disconnect { during_commit, .. } => {
                let msg = err.to_string();
                TestKitError::DriverError {
                    error_type: String::from(if during_commit {
                        "IncompleteCommit"
                    } else {
                        "Disconnect"
                    }),
                    msg,
                    code: None,
                    id: None,
                    retryable,
                }
            }
            Neo4jError::InvalidConfig { message, .. } => TestKitError::DriverError {
                error_type: String::from("ConfigError"),
                msg: message,
                code: None,
                id: None,
                retryable,
            },
            Neo4jError::ServerError { error, .. } => {
                let ServerError { code, message, .. } = error;
                TestKitError::DriverError {
                    error_type: String::from("ServerError"),
                    msg: message,
                    code: Some(code),
                    id: None,
                    retryable,
                }
            }
            Neo4jError::Timeout { message, .. } => TestKitError::DriverError {
                error_type: String::from("Timeout"),
                msg: message,
                code: None,
                id: None,
                retryable,
            },
            Neo4jError::UserCallback { error, .. } => error.into(),
            Neo4jError::ProtocolError { message, .. } => TestKitError::DriverError {
                error_type: String::from("ProtocolError"),
                msg: message,
                code: None,
                id: None,
                retryable,
            },
        }
    }
}

impl From<UserCallbackError> for TestKitError {
    fn from(value: UserCallbackError) -> Self {
        match value {
            UserCallbackError::Resolver(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected resolver error: {}", err),
                },
            },
            UserCallbackError::AuthManager(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected auth manager error: {}", err),
                },
            },
            UserCallbackError::BookmarkManagerGet(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager get error: {}", err),
                },
            },
            UserCallbackError::BookmarkManagerUpdate(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager update error: {}", err),
                },
            },
            _ => TestKitError::BackendError {
                msg: format!("unhandled user callback error type: {}", value),
            },
        }
    }
}

impl From<&UserCallbackError> for TestKitError {
    fn from(value: &UserCallbackError) -> Self {
        match value {
            UserCallbackError::Resolver(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected resolver error: {}", err),
                },
            },
            UserCallbackError::AuthManager(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected auth manager error: {}", err),
                },
            },
            UserCallbackError::BookmarkManagerGet(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager get error: {}", err),
                },
            },
            UserCallbackError::BookmarkManagerUpdate(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager update error: {}", err),
                },
            },
            _ => TestKitError::BackendError {
                msg: format!("unhandled user callback error type: {}", value),
            },
        }
    }
}

impl From<serde_json::Error> for TestKitError {
    fn from(err: serde_json::Error) -> Self {
        TestKitError::BackendError {
            msg: format!("unexpected message format: {err:?}"),
        }
    }
}

impl From<NotADriverValueError> for TestKitError {
    fn from(v: NotADriverValueError) -> Self {
        TestKitError::BackendError {
            msg: format!("{v}"),
        }
    }
}

impl From<BrokenValueError> for TestKitError {
    fn from(v: BrokenValueError) -> Self {
        // TestKit expects broken values to be represented as error when accessing the field.
        // Instead, the Rust driver exposes them a an enum variant.
        // Hence we just re-write that variant as an error.
        TestKitError::DriverError {
            error_type: String::from("BrokenValueError"),
            msg: format!("{v}"),
            code: None,
            id: None,
            retryable: false,
        }
    }
}

impl<Builder> From<ConfigureFetchSizeError<Builder>> for TestKitError {
    fn from(e: ConfigureFetchSizeError<Builder>) -> Self {
        TestKitError::DriverError {
            error_type: String::from("ConfigureFetchSizeError"),
            msg: format!("{e}"),
            code: None,
            id: None,
            retryable: false,
        }
    }
}

impl From<ConnectionConfigParseError> for TestKitError {
    fn from(e: ConnectionConfigParseError) -> Self {
        TestKitError::DriverError {
            error_type: String::from("ConnectionConfigParseError"),
            msg: format!("{e}"),
            code: None,
            id: None,
            retryable: false,
        }
    }
}

impl From<TlsConfigError> for TestKitError {
    fn from(e: TlsConfigError) -> Self {
        TestKitError::DriverError {
            error_type: String::from("TlsConfigError"),
            msg: format!("{e}"),
            code: None,
            id: None,
            retryable: false,
        }
    }
}

impl From<RetryError> for TestKitError {
    fn from(v: RetryError) -> Self {
        match v {
            RetryError::Neo4jError(e) => e.into(),
            RetryError::Timeout(e) => TestKitError::DriverError {
                error_type: String::from("RetryableError"),
                msg: format!("{e}"),
                code: None,
                id: None,
                retryable: false,
            },
        }
    }
}

impl Error for TestKitError {}

impl TestKitError {
    pub(super) fn set_id_gen(&mut self, generator: &Generator) {
        match self {
            TestKitError::DriverError { id, .. } => {
                *id = Some(generator.next_id());
            }
            TestKitError::FatalError { .. }
            | TestKitError::FrontendError { .. }
            | TestKitError::BackendError { .. } => {}
        }
    }

    pub(super) fn set_id(&mut self, new_id: BackendId) {
        match self {
            TestKitError::DriverError { id, .. } => {
                *id = Some(new_id);
            }
            TestKitError::FatalError { .. }
            | TestKitError::FrontendError { .. }
            | TestKitError::BackendError { .. } => {}
        }
    }

    pub(super) fn get_id(&self) -> Option<BackendId> {
        match self {
            TestKitError::DriverError { id, .. } => *id,
            TestKitError::FatalError { .. }
            | TestKitError::FrontendError { .. }
            | TestKitError::BackendError { .. } => None,
        }
    }

    pub(super) fn wrap_fatal<T, E: Error + Debug>(res: Result<T, E>) -> Result<T, Self> {
        match res {
            Ok(ok) => Ok(ok),
            Err(err) => Err(Self::FatalError {
                error: format!("{:?}", err),
            }),
        }
    }

    pub(super) fn backend_err<S: Into<String>>(message: S) -> Self {
        Self::BackendError {
            msg: message.into(),
        }
    }

    pub(super) fn clone_neo4j_error(err: &Neo4jError) -> Self {
        let retryable = err.is_retryable();
        match err {
            Neo4jError::Disconnect { during_commit, .. } => {
                let msg = err.to_string();
                TestKitError::DriverError {
                    error_type: String::from(if *during_commit {
                        "IncompleteCommit"
                    } else {
                        "Disconnect"
                    }),
                    msg,
                    code: None,
                    id: None,
                    retryable,
                }
            }
            Neo4jError::InvalidConfig { message, .. } => TestKitError::DriverError {
                error_type: String::from("ConfigError"),
                msg: message.clone(),
                code: None,
                id: None,
                retryable,
            },
            Neo4jError::ServerError { error, .. } => TestKitError::DriverError {
                error_type: String::from("ServerError"),
                msg: String::from(error.message()),
                code: Some(String::from(error.code())),
                id: None,
                retryable,
            },
            Neo4jError::Timeout { message, .. } => TestKitError::DriverError {
                error_type: String::from("Timeout"),
                msg: message.clone(),
                code: None,
                id: None,
                retryable,
            },
            Neo4jError::UserCallback { error, .. } => error.into(),
            Neo4jError::ProtocolError { message, .. } => TestKitError::DriverError {
                error_type: String::from("ProtocolError"),
                msg: message.clone(),
                code: None,
                id: None,
                retryable,
            },
        }
    }
}
