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

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

use crate::driver::{ConnectionConfig, Driver, DriverConfig};
use crate::testkit_backend::errors::TestKitError;

use super::responses::Response;
use super::{Backend, BackendId, TestKitResult};

#[derive(Deserialize, Debug)]
#[serde(tag = "name", content = "data", deny_unknown_fields)]
pub(crate) enum Request {
    #[serde(rename_all = "camelCase")]
    StartTest {
        test_name: String,
    },
    StartSubTest {
        #[serde(rename = "testName")]
        test_name: String,
        #[serde(rename = "subtestArguments")]
        arguments: HashMap<String, Value>,
    },
    GetFeatures {},
    #[serde(rename_all = "camelCase")]
    NewDriver {
        uri: String,
        #[serde(rename = "authorizationToken")]
        auth: RequestAuth,
        user_agent: Option<String>,
        resolver_registered: Option<bool>,
        #[serde(rename = "domainNameResolverRegistered")]
        dns_registered: Option<bool>,
        connection_timeout_ms: Option<u64>,
        fetch_size: Option<usize>,
        max_tx_retry_time_ms: Option<u64>,
        liveness_check_timeout_ms: Option<u64>,
        max_connection_pool_size: Option<usize>,
        connection_acquisition_timeout_ms: Option<usize>,
        notifications_min_severity: Option<String>,
        notifications_disabled_categories: Option<Vec<String>>,
        encrypted: Option<bool>,
        trusted_certificates: Option<RequestTrustedCertificates>,
    },
    #[serde(rename_all = "camelCase")]
    VerifyConnectivity {
        driver_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    GetServerInfo {
        driver_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    CheckMultiDBSupport {
        driver_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    CheckDriverIsEncrypted {
        driver_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResolverResolutionCompleted {
        request_id: BackendId,
        addresses: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    BookmarksSupplierCompleted {
        request_id: usize,
        bookmarks: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    BookmarksConsumerCompleted {
        request_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    NewBookmarkManager {
        initial_bookmarks: Vec<String>,
        bookmarks_supplier_registered: bool,
        bookmarks_consumer_registered: bool,
    },
    #[serde(rename_all = "camelCase")]
    BookmarkManagerClose {
        id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    DomainNameResolutionCompleted {
        request_id: BackendId,
        addresses: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    DriverClose {
        driver_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    NewSession {
        driver_id: BackendId,
        access_mode: RequestAccessMode,
        bookmarks: Option<Vec<String>>,
        database: Option<String>,
        fetch_size: Option<usize>,
        impersonated_user: Option<String>,
        notifications_min_severity: Option<String>,
        notifications_disabled_categories: Option<String>,
        bookmark_manager_id: Option<BackendId>,
    },
    #[serde(rename_all = "camelCase")]
    SessionClose {
        session_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    SessionRun {
        session_id: BackendId,
        #[serde(rename = "cypher")]
        query: String,
        params: Option<HashMap<String, Value>>,
        tx_meta: Option<HashMap<String, Value>>,
        timeout: Option<u64>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(crate) enum RequestAuth {
    AuthorizationToken {
        scheme: String,
        #[serde(flatten)]
        data: HashMap<String, Value>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub(crate) enum RequestTrustedCertificates {
    Const(String),
    Paths(Vec<String>),
}

#[derive(Deserialize, Debug)]
pub(crate) enum RequestAccessMode {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "w")]
    Write,
}

impl Request {
    pub(crate) fn handle(self, backend: &mut Backend) -> TestKitResult {
        match self {
            Request::StartTest { .. } => backend.send(&Response::RunTest)?,
            // Request::StartSubTest
            Request::GetFeatures {} => backend.send(&Response::feature_list())?,
            Request::NewDriver { .. } => self.new_driver(backend)?,
            _ => backend.send(&TestKitError::BackendError {
                msg: format!("Unhandled request {:?}", self),
            })?,
        }
        Ok(())
    }

    fn new_driver(self, backend: &mut Backend) -> TestKitResult {
        let Request::NewDriver {
            uri,
            auth,
            user_agent,
            resolver_registered,
            dns_registered,
            connection_timeout_ms,
            fetch_size,
            max_tx_retry_time_ms,
            liveness_check_timeout_ms,
            max_connection_pool_size,
            connection_acquisition_timeout_ms,
            notifications_min_severity,
            notifications_disabled_categories,
            encrypted,
            trusted_certificates,
        } = self else {
            panic!("expected Request::NewDriver");
        };
        let connection_config: ConnectionConfig = uri.as_str().try_into()?;
        let mut driver_config = DriverConfig::new();
        driver_config = set_auth(driver_config, auth)?;
        if let Some(user_agent) = user_agent {
            driver_config = driver_config.with_user_agent(user_agent);
        }
        if resolver_registered.unwrap_or(false) {
            return Err(TestKitError::backend_err("resolver unsupported"));
        }
        if dns_registered.unwrap_or(false) {
            return Err(TestKitError::backend_err("DNS resolver unsupported"));
        }
        if connection_timeout_ms.is_some() {
            return Err(TestKitError::backend_err("connection timeout unsupported"));
        }
        if fetch_size.is_some() {
            return Err(TestKitError::backend_err("fetch size unsupported"));
        }
        if max_tx_retry_time_ms.is_some() {
            return Err(TestKitError::backend_err("max tx retry time unsupported"));
        }
        if liveness_check_timeout_ms.is_some() {
            return Err(TestKitError::backend_err("liveness check unsupported"));
        }
        if let Some(max_connection_pool_size) = max_connection_pool_size {
            driver_config = driver_config.with_max_connection_pool_size(max_connection_pool_size);
        }
        if connection_acquisition_timeout_ms.is_some() {
            return Err(TestKitError::backend_err(
                "connection acquisition timeout unsupported",
            ));
        }
        if notifications_min_severity.is_some() {
            return Err(TestKitError::backend_err(
                "notification severity filter unsupported",
            ));
        }
        if notifications_disabled_categories.is_some() {
            return Err(TestKitError::backend_err(
                "notification category filter unsupported",
            ));
        }
        if encrypted.is_some() {
            return Err(TestKitError::backend_err(
                "explicit encryption config unsupported",
            ));
        }
        if trusted_certificates.is_some() {
            return Err(TestKitError::backend_err("CA config unsupported"));
        }
        let driver = Driver::new(connection_config, driver_config);
        let id = backend.next_id();
        backend.drivers.insert(id, driver);
        backend.send(&Response::Driver { id })?;
        Ok(())
    }
}

fn set_auth(mut config: DriverConfig, auth: RequestAuth) -> Result<DriverConfig, TestKitError> {
    let RequestAuth::AuthorizationToken { scheme, mut data } = auth;
    match scheme.as_str() {
        "basic" => {
            let Value::String(principal) = data.remove("principal").ok_or_else(|| {
                TestKitError::backend_err("auth: basic scheme required principal")
            })? else {
                return Err(TestKitError::backend_err("auth: principal needs to be string"));
            };
            let Value::String(credentials) = data.remove("credentials").ok_or_else(|| {
                TestKitError::backend_err("auth: basic scheme required credentials")
            })? else {
                return Err(TestKitError::backend_err("auth: credentials needs to be string"));
            };
            let realm = data
                .remove("realm")
                .map(|v| match v {
                    Value::String(v) => Ok(v),
                    _ => Err(TestKitError::backend_err("auth: reaml needs t obe string")),
                })
                .unwrap_or(Ok(String::new()))?;
            config = config.with_basic_auth(&principal, &credentials, &realm)
        }
        _ => return Err(TestKitError::backend_err("unsupported scheme")),
    }
    Ok(config)
}
