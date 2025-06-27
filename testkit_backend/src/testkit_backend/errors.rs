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

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use neo4j::driver::{ConfigureFetchSizeError, ConnectionConfigParseError, TlsConfigError};
use neo4j::error::{GqlErrorCause, GqlErrorClassification, ServerError, UserCallbackError};
use neo4j::retry::RetryError;
use neo4j::{Neo4jError, ValueReceive};

use super::cypher_value::{BrokenValueError, NotADriverValueError};
use super::{BackendId, Generator};

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)] // names are reflecting TestKit protocol
pub(super) enum TestKitError {
    DriverError {
        error: Box<TestKitDriverError>,
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

#[derive(Debug, Clone)]
pub(super) struct TestKitDriverError {
    pub(super) error_type: String,
    pub(super) msg: String,
    pub(super) code: Option<String>,
    pub(super) gql_status: Option<String>,
    pub(super) status_description: Option<String>,
    pub(super) cause: Option<Box<GqlErrorCause>>,
    pub(super) diagnostic_record: Option<HashMap<String, ValueReceive>>,
    pub(super) classification: Option<GqlErrorClassification>,
    pub(super) raw_classification: Option<String>,
    pub(super) id: Option<BackendId>,
    pub(super) retryable: bool,
}

impl TestKitError {
    pub(super) fn driver_error_client_only(
        error_type: String,
        msg: String,
        retryable: bool,
    ) -> Self {
        TestKitDriverError {
            error_type,
            msg,
            code: None,
            gql_status: None,
            status_description: None,
            cause: None,
            diagnostic_record: None,
            classification: None,
            raw_classification: None,
            id: None,
            retryable,
        }
        .into()
    }
}

impl From<TestKitDriverError> for TestKitError {
    fn from(error: TestKitDriverError) -> Self {
        TestKitError::DriverError {
            error: Box::new(error),
        }
    }
}

impl Display for TestKitError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<Neo4jError> for TestKitError {
    fn from(err: Neo4jError) -> Self {
        let retryable = err.is_retryable();
        match err {
            Neo4jError::Disconnect { during_commit, .. } => {
                let msg = err.to_string();
                TestKitError::driver_error_client_only(
                    String::from(if during_commit {
                        "IncompleteCommit"
                    } else {
                        "Disconnect"
                    }),
                    msg,
                    retryable,
                )
            }
            Neo4jError::InvalidConfig { message, .. } => TestKitError::driver_error_client_only(
                String::from("ConfigError"),
                message,
                retryable,
            ),
            Neo4jError::ServerError { error, .. } => {
                let ServerError {
                    code,
                    gql_status,
                    message,
                    gql_status_description,
                    gql_raw_classification,
                    gql_classification,
                    diagnostic_record,
                    cause,
                    ..
                } = *error;
                TestKitDriverError {
                    error_type: String::from("ServerError"),
                    msg: message,
                    code: Some(code),
                    gql_status: Some(gql_status),
                    status_description: Some(gql_status_description),
                    cause,
                    diagnostic_record: Some(diagnostic_record),
                    classification: Some(gql_classification),
                    raw_classification: gql_raw_classification,
                    id: None,
                    retryable,
                }
                .into()
            }
            Neo4jError::Timeout { message, .. } => {
                TestKitError::driver_error_client_only(String::from("Timeout"), message, retryable)
            }
            Neo4jError::UserCallback { error, .. } => error.into(),
            Neo4jError::ProtocolError { message, .. } => TestKitError::driver_error_client_only(
                String::from("ProtocolError"),
                message,
                retryable,
            ),
        }
    }
}

impl From<UserCallbackError> for TestKitError {
    fn from(value: UserCallbackError) -> Self {
        match value {
            UserCallbackError::Resolver(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected resolver error: {err}"),
                },
            },
            UserCallbackError::AuthManager(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected auth manager error: {err}"),
                },
            },
            UserCallbackError::BookmarkManagerGet(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager get error: {err}"),
                },
            },
            UserCallbackError::BookmarkManagerUpdate(err) => match err.downcast::<Self>() {
                Ok(err) => *err,
                Err(err) => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager update error: {err}"),
                },
            },
            _ => TestKitError::BackendError {
                msg: format!("unhandled user callback error type: {value}"),
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
                    msg: format!("unexpected resolver error: {err}"),
                },
            },
            UserCallbackError::AuthManager(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected auth manager error: {err}"),
                },
            },
            UserCallbackError::BookmarkManagerGet(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager get error: {err}"),
                },
            },
            UserCallbackError::BookmarkManagerUpdate(err) => match err.downcast_ref::<Self>() {
                Some(err) => err.clone(),
                None => TestKitError::BackendError {
                    msg: format!("unexpected bookmark manager update error: {err}"),
                },
            },
            _ => TestKitError::BackendError {
                msg: format!("unhandled user callback error type: {value}"),
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
        // Hence, we just re-write that variant as an error.
        TestKitError::driver_error_client_only(
            String::from("BrokenValueError"),
            format!("{v}"),
            false,
        )
    }
}

impl<Builder> From<ConfigureFetchSizeError<Builder>> for TestKitError {
    fn from(e: ConfigureFetchSizeError<Builder>) -> Self {
        TestKitError::driver_error_client_only(
            String::from("ConfigureFetchSizeError"),
            format!("{e}"),
            false,
        )
    }
}

impl From<ConnectionConfigParseError> for TestKitError {
    fn from(e: ConnectionConfigParseError) -> Self {
        TestKitError::driver_error_client_only(
            String::from("ConnectionConfigParseError"),
            format!("{e}"),
            false,
        )
    }
}

impl From<TlsConfigError> for TestKitError {
    fn from(e: TlsConfigError) -> Self {
        TestKitError::driver_error_client_only(
            String::from("TlsConfigError"),
            format!("{e}"),
            false,
        )
    }
}

impl From<RetryError> for TestKitError {
    fn from(v: RetryError) -> Self {
        match v {
            RetryError::Neo4jError(e) => e.into(),
            RetryError::Timeout(e) => TestKitError::driver_error_client_only(
                String::from("RetryableError"),
                format!("{e}"),
                false,
            ),
        }
    }
}

impl Error for TestKitError {}

impl TestKitError {
    pub(super) fn set_id_gen(&mut self, generator: &Generator) {
        match self {
            TestKitError::DriverError { error } => {
                error.id = Some(generator.next_id());
            }
            TestKitError::FatalError { .. }
            | TestKitError::FrontendError { .. }
            | TestKitError::BackendError { .. } => {}
        }
    }

    pub(super) fn set_id(&mut self, new_id: BackendId) {
        match self {
            TestKitError::DriverError { error } => {
                error.id = Some(new_id);
            }
            TestKitError::FatalError { .. }
            | TestKitError::FrontendError { .. }
            | TestKitError::BackendError { .. } => {}
        }
    }

    pub(super) fn get_id(&self) -> Option<BackendId> {
        match self {
            TestKitError::DriverError { error } => error.id,
            TestKitError::FatalError { .. }
            | TestKitError::FrontendError { .. }
            | TestKitError::BackendError { .. } => None,
        }
    }

    pub(super) fn wrap_fatal<T, E: Error + Debug>(res: Result<T, E>) -> Result<T, Self> {
        match res {
            Ok(ok) => Ok(ok),
            Err(err) => Err(Self::FatalError {
                error: format!("{err:?}"),
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
                TestKitError::driver_error_client_only(
                    String::from(if *during_commit {
                        "IncompleteCommit"
                    } else {
                        "Disconnect"
                    }),
                    msg,
                    retryable,
                )
            }
            Neo4jError::InvalidConfig { message, .. } => TestKitError::driver_error_client_only(
                String::from("ConfigError"),
                message.clone(),
                retryable,
            ),
            Neo4jError::ServerError { error, .. } => TestKitDriverError {
                error_type: String::from("ServerError"),
                msg: error.message.clone(),
                code: Some(error.code.clone()),
                gql_status: Some(error.gql_status.clone()),
                status_description: Some(error.gql_status_description.clone()),
                cause: error.cause.clone(),
                diagnostic_record: Some(error.diagnostic_record.clone()),
                classification: Some(error.gql_classification),
                raw_classification: error.gql_raw_classification.clone(),
                id: None,
                retryable,
            }
            .into(),
            Neo4jError::Timeout { message, .. } => TestKitError::driver_error_client_only(
                String::from("Timeout"),
                message.clone(),
                retryable,
            ),
            Neo4jError::UserCallback { error, .. } => error.into(),
            Neo4jError::ProtocolError { message, .. } => TestKitError::driver_error_client_only(
                String::from("ProtocolError"),
                message.clone(),
                retryable,
            ),
        }
    }
}
