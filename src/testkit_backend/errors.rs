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

use crate::Neo4jError;

use super::cypher_value::{BrokenValueError, NotADriverValueError};

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum TestKitError {
    DriverError {
        error_type: String,
        msg: String,
        code: Option<String>,
    },
    FrontendError {
        msg: String,
    },
    BackendError {
        msg: String,
    },
    FatalError {
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
        match err {
            Neo4jError::Disconnect {
                message,
                source,
                during_commit,
            } => TestKitError::DriverError {
                error_type: String::from(if during_commit {
                    "IncompleteCommitError"
                } else {
                    "DriverError"
                }),
                msg: match source {
                    None => message,
                    Some(source) => format!("{}: {}", message, source),
                },
                code: None,
            },
            Neo4jError::InvalidConfig { message } => TestKitError::DriverError {
                error_type: String::from("ConfigError"),
                msg: message,
                code: None,
            },
            Neo4jError::ServerError { error } => TestKitError::DriverError {
                error_type: String::from("ServerError"),
                msg: String::from(error.message()),
                code: Some(String::from(error.code())),
            },
            Neo4jError::ProtocolError { message } => TestKitError::DriverError {
                error_type: String::from("ProtocolError"),
                msg: message,
                code: None,
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
        }
    }
}

impl Error for TestKitError {}

impl TestKitError {
    pub(crate) fn wrap_fatal<T, E: Error + Debug>(res: Result<T, E>) -> Result<T, Self> {
        match res {
            Ok(ok) => Ok(ok),
            Err(err) => Err(Self::FatalError {
                error: format!("{:?}", err),
            }),
        }
    }

    pub(crate) fn backend_err<S: Into<String>>(message: S) -> Self {
        Self::BackendError {
            msg: message.into(),
        }
    }
}
