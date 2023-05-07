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

use crate::bookmarks::Bookmarks;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::driver::{ConnectionConfig, DriverConfig, RoutingControl};
use crate::session::SessionConfig;

use super::cypher_value::CypherValue;
use super::driver_holder::{CloseSession, DriverHolder, NewSession};
use super::errors::TestKitError;
use super::responses::Response;
use super::session_holder::{AutoCommit, ResultConsume, ResultNext};
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
        auth: TestKitAuth,
        auth_token_manager_id: Option<BackendId>,
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
        initial_bookmarks: Option<Vec<String>>,
        bookmarks_supplier_registered: Option<bool>,
        bookmarks_consumer_registered: Option<bool>,
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
        notifications_disabled_categories: Option<Vec<String>>,
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
        params: Option<HashMap<String, CypherValue>>,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionReadTransaction {
        session_id: BackendId,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionWriteTransaction {
        session_id: BackendId,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionBeginTransaction {
        session_id: BackendId,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<u64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionLastBookmarks {
        session_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    TransactionRun {
        #[serde(rename = "txId")]
        transaction_id: BackendId,
        #[serde(rename = "cypher")]
        query: String,
        params: Option<HashMap<String, CypherValue>>,
    },
    #[serde(rename_all = "camelCase")]
    TransactionCommit {
        #[serde(rename = "txId")]
        transaction_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    TransactionRollback {
        #[serde(rename = "txId")]
        transaction_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    TransactionClose {
        #[serde(rename = "txId")]
        transaction_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResultNext {
        result_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResultSingle {
        result_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResultSingleOptional {
        result_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResultPeek {
        result_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResultConsume {
        result_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ResultList {
        result_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    RetryablePositive {
        session_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    RetryableNegative {
        session_id: BackendId,
        error_id: BackendErrorId,
    },
    #[serde(rename_all = "camelCase")]
    ForcedRoutingTableUpdate {
        driver_id: BackendId,
        database: Option<String>,
        bookmarks: Option<Vec<String>>,
    },
    #[serde(rename_all = "camelCase")]
    GetRoutingTable {
        driver_id: BackendId,
        database: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    GetConnectionPoolMetrics {
        driver_id: BackendId,
        address: String,
    },
    // Currently unused and fields are not well documented
    // #[serde(rename_all = "camelCase")]
    // CypherTypeField {
    //     // ...
    // },
    #[serde(rename_all = "camelCase")]
    ExecuteQuery {
        driver_id: BackendId,
        #[serde(rename = "cypher")]
        query: String,
        config: Option<ExecuteQueryConfig>,
    },
    FakeTimeInstall {},
    #[serde(rename_all = "camelCase")]
    FakeTimeTick {
        increment_ms: i64,
    },
    FakeTimeUninstall {},
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(crate) enum TestKitAuth {
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
#[serde(untagged)]
pub(crate) enum BackendErrorId {
    BackendError(BackendId),
    #[serde(deserialize_with = "deserialize_client_error_id")]
    ClientError,
}

fn deserialize_client_error_id<'de, D>(d: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(d)?.as_str() {
        "" => Ok(()),
        _ => Err(D::Error::custom("client error must be represented as \"\"")),
    }
}

#[derive(Deserialize, Debug)]
pub(crate) enum RequestAccessMode {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "w")]
    Write,
}

impl From<RequestAccessMode> for RoutingControl {
    fn from(mode: RequestAccessMode) -> Self {
        match mode {
            RequestAccessMode::Read => RoutingControl::Read,
            RequestAccessMode::Write => RoutingControl::Write,
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExecuteQueryConfig {
    database: Option<String>,
    routing: Option<RequestAccessMode>,
    impersonated_user: Option<String>,
    bookmark_manager_id: Option<ExecuteQueryBmmId>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub(crate) enum ExecuteQueryBmmId {
    BackendId(BackendId),
    #[serde(deserialize_with = "deserialize_default_bmm_id")]
    Default,
}

fn deserialize_default_bmm_id<'de, D>(d: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    match i8::deserialize(d)? {
        -1 => Ok(()),
        _ => Err(D::Error::custom("default BMM ID must be -1")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn foo() {
        let r: Result<Request, _> = serde_json::from_str(
            "{
    \"name\": \"NewDriver\",
    \"data\": {
        \"uri\": \"foo\",
        \"authorizationToken\": {
            \"name\": \"AuthorizationToken\",
            \"data\": {
                \"scheme\": \"basic\"
            }
        }
    }
}
        ",
        );
        let r = r.unwrap();
        println!("{r:?}");
    }
}

impl Request {
    pub(crate) fn handle(self, backend: &mut Backend) -> TestKitResult {
        match self {
            Request::StartTest { .. } => backend.send(&Response::RunTest)?,
            // Request::StartSubTest
            Request::GetFeatures { .. } => backend.send(&Response::feature_list())?,
            Request::NewDriver { .. } => self.new_driver(backend)?,
            // Request::VerifyConnectivity { .. } => {},
            // Request::GetServerInfo { .. } => {},
            // Request::CheckMultiDBSupport { .. } => {},
            // Request::CheckDriverIsEncrypted { .. } => {},
            // Request::ResolverResolutionCompleted { .. } => {},
            // Request::BookmarksSupplierCompleted { .. } => {},
            // Request::BookmarksConsumerCompleted { .. } => {},
            // Request::NewBookmarkManager { .. } => {},
            // Request::BookmarkManagerClose { .. } => {},
            // Request::DomainNameResolutionCompleted { .. } => {},
            Request::DriverClose { .. } => self.driver_close(backend)?,
            Request::NewSession { .. } => self.new_session(backend)?,
            Request::SessionClose { .. } => self.close_session(backend)?,
            Request::SessionRun { .. } => self.session_auto_commit(backend)?,
            // Request::SessionReadTransaction { .. } => {},
            // Request::SessionWriteTransaction { .. } => {},
            // Request::SessionBeginTransaction { .. } => {},
            // Request::SessionLastBookmarks { .. } => {},
            // Request::TransactionRun { .. } => {},
            // Request::TransactionCommit { .. } => {},
            // Request::TransactionRollback { .. } => {},
            // Request::TransactionClose { .. } => {},
            Request::ResultNext { .. } => self.result_next(backend)?,
            // Request::ResultSingle { .. } => {},
            // Request::ResultSingleOptional { .. } => {},
            // Request::ResultPeek { .. } => {},
            Request::ResultConsume { .. } => self.result_consume(backend)?,
            // Request::ResultList { .. } => {},
            // Request::RetryablePositive { .. } => {},
            // Request::RetryableNegative { .. } => {},
            // Request::ForcedRoutingTableUpdate { .. } => {},
            // Request::GetRoutingTable { .. } => {},
            // Request::GetConnectionPoolMetrics { .. } => {},
            // Request::ExecuteQuery { .. } => {},
            // Request::FakeTimeInstall { .. } => {},
            // Request::FakeTimeTick { .. } => {},
            // Request::FakeTimeUninstall { .. } => {},
            _ => {
                return Err(TestKitError::backend_err(format!(
                    "Unhandled request {:?}",
                    self
                )))
            }
        }
        Ok(())
    }

    fn new_driver(self, backend: &mut Backend) -> TestKitResult {
        let Request::NewDriver {
            uri,
            auth,
            auth_token_manager_id,
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
        if auth_token_manager_id.is_some() {
            return Err(TestKitError::backend_err("auth token manager unsupported"));
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
        // let driver = Driver::new(connection_config, driver_config);
        let driver_holder = DriverHolder::new(
            backend.id_generator.clone(),
            connection_config,
            driver_config,
        );
        let id = backend.next_id();
        backend.drivers.insert(id, driver_holder);
        backend.send(&Response::Driver { id })
    }

    fn driver_close(self, backend: &mut Backend) -> TestKitResult {
        let Request::DriverClose {
            driver_id
        } = self else {
            panic!("expected Request::DriverClose");
        };
        let Some(driver_holder) = backend.drivers.remove(&driver_id) else {
            return Err(TestKitError::backend_err(format!("No driver with id {driver_id} found")));
        };
        drop(driver_holder);
        backend.send(&Response::Driver { id: driver_id })
    }

    fn new_session(self, backend: &mut Backend) -> TestKitResult {
        let Request::NewSession {
            driver_id, access_mode, bookmarks, database, fetch_size, impersonated_user, notifications_min_severity, notifications_disabled_categories, bookmark_manager_id,
        } = self else {
            panic!("expected Request::NewDriver");
        };
        let Some(driver_holder) = backend.drivers.get(&driver_id) else {
            return Err(TestKitError::backend_err(format!("No driver with id {driver_id} found")));
        };
        let mut config = SessionConfig::new();
        if let Some(bookmarks) = bookmarks {
            config = config.with_bookmarks(Bookmarks::from_raw(bookmarks));
        }
        if let Some(database) = database {
            config = config.with_database(database);
        }
        if let Some(fetch_size) = fetch_size {
            return Err(TestKitError::backend_err(format!(
                "Driver does not yet support custom fetch_size, found {fetch_size}"
            )));
        }
        if let Some(imp_user) = impersonated_user {
            config = config.with_impersonated_user(imp_user);
        }
        if let Some(notifications_min_severity) = notifications_min_severity {
            return Err(TestKitError::backend_err(format!("Driver does not yet support notifications_min_severity, found {notifications_min_severity}")));
        }
        if let Some(notifications_disabled_categories) = notifications_disabled_categories {
            return Err(TestKitError::backend_err(format!("Driver does not yet support notifications_disabled_categories, found {notifications_disabled_categories:?}")));
        }
        if let Some(bookmark_manager_id) = bookmark_manager_id {
            return Err(TestKitError::backend_err(format!(
                "Driver does not yet support bookmark_manager_id, found {bookmark_manager_id}"
            )));
        }
        let id = driver_holder
            .session(NewSession {
                auto_commit_access_mode: access_mode.into(),
                config,
            })
            .session_id;
        backend.session_id_to_driver_id.insert(id, driver_id);
        backend.send(&Response::Session { id })
    }

    fn close_session(self, backend: &mut Backend) -> TestKitResult {
        let Request::SessionClose { session_id } = self else {
            panic!("expected Request::SessionClose");
        };
        let Some(driver_id) = backend.session_id_to_driver_id.remove(&session_id) else {
            return Err(TestKitError::backend_err(format!("Unknown session id {} in backend", session_id)));
        };
        backend
            .drivers
            .get(&driver_id)
            .unwrap()
            .session_close(CloseSession { session_id })
            .result?;
        backend.send(&Response::Session { id: session_id })
    }

    fn session_auto_commit(self, backend: &mut Backend) -> TestKitResult {
        let Request::SessionRun { session_id, query, params, tx_meta, timeout } = self else {
            panic!("expected Request::SessionRun");
        };
        let Some(&driver_id) = backend.session_id_to_driver_id.get(&session_id) else {
            return Err(TestKitError::backend_err(format!("Unknown session id {} in backend", session_id)));
        };
        let params = params
            .map(|params| {
                params
                    .into_iter()
                    .map(|(k, v)| Ok::<_, TestKitError>((k, v.try_into()?)))
                    .collect::<Result<_, _>>()
            })
            .transpose()?;
        let (result_id, keys) = backend
            .drivers
            .get(&driver_id)
            .unwrap()
            .auto_commit(AutoCommit {
                session_id,
                query,
                params,
                tx_meta: None,
                timeout: None,
            })
            .result?;
        backend.result_id_to_driver_id.insert(result_id, driver_id);
        backend.send(&Response::Result {
            id: result_id,
            keys: keys.into_iter().map(|k| (*k).clone()).collect(),
        })
    }

    fn result_next(self, backend: &mut Backend) -> TestKitResult {
        let Request::ResultNext { result_id } = self else {
            panic!("expected Request::ResultNext");
        };
        let Some(&driver_id) = backend.result_id_to_driver_id.get(&result_id) else {
            return Err(TestKitError::backend_err(format!("Unknown result id {result_id} in backend")));
        };
        let response = match backend
            .drivers
            .get(&driver_id)
            .unwrap()
            .result_next(ResultNext { result_id })
            .result?
            .transpose()?
        {
            None => Response::NullRecord,
            Some(record) => Response::Record {
                values: record
                    .entries
                    .into_iter()
                    .map(|(_, v)| v.try_into())
                    .collect::<Result<_, _>>()?,
            },
        };
        backend.send(&response)
    }

    fn result_consume(self, backend: &mut Backend) -> TestKitResult {
        let Request::ResultConsume { result_id } = self else {
            panic!("expected Request::ResultConsume");
        };
        let Some(&driver_id) = backend.result_id_to_driver_id.get(&result_id) else {
            return Err(TestKitError::backend_err(format!("Unknown result id {result_id} in backend")));
        };
        let summary = backend
            .drivers
            .get(&driver_id)
            .unwrap()
            .result_consume(ResultConsume { result_id })
            .result??;
        backend.send(&Response::Summary(summary.try_into()?))
    }
}

fn set_auth(mut config: DriverConfig, auth: TestKitAuth) -> Result<DriverConfig, TestKitError> {
    let TestKitAuth::AuthorizationToken { scheme, mut data } = auth;
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
