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
use std::sync::Arc;
use std::time::Duration;

use crate::driver::{ConnectionConfig, DriverConfig, RoutingControl};
use crate::session::SessionConfig;
use crate::testkit_backend::session_holder::RetryableOutcome;
use crate::ValueSend;

use super::cypher_value::CypherValue;
use super::driver_holder::{CloseSession, DriverHolder, EmulatedDriverConfig, NewSession};
use super::errors::TestKitError;
use super::resolver::TestKitResolver;
use super::responses::Response;
use super::session_holder::{
    AutoCommit, BeginTransaction, CloseTransaction, CommitTransaction, LastBookmarks,
    ResultConsume, ResultNext, ResultSingle, RetryableNegative, RetryablePositive,
    RollbackTransaction, TransactionFunction, TransactionRun,
};
use super::{Backend, BackendData, BackendId, TestKitResult};

#[derive(Deserialize, Debug)]
#[serde(tag = "name", content = "data", deny_unknown_fields)]
pub(super) enum Request {
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
        fetch_size: Option<i64>,
        max_tx_retry_time_ms: Option<u64>,
        liveness_check_timeout_ms: Option<u64>,
        max_connection_pool_size: Option<usize>,
        connection_acquisition_timeout_ms: Option<u64>,
        notifications_min_severity: Option<String>,
        notifications_disabled_categories: Option<Vec<String>>,
        encrypted: Option<bool>,
        trusted_certificates: Option<Vec<String>>,
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
        fetch_size: Option<i64>,
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
        timeout: Option<i64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionReadTransaction {
        session_id: BackendId,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<i64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionWriteTransaction {
        session_id: BackendId,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<i64>,
    },
    #[serde(rename_all = "camelCase")]
    SessionBeginTransaction {
        session_id: BackendId,
        tx_meta: Option<HashMap<String, CypherValue>>,
        timeout: Option<i64>,
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
pub(super) enum TestKitAuth {
    AuthorizationToken {
        scheme: String,
        #[serde(flatten)]
        data: HashMap<String, Value>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub(super) enum RequestTrustedCertificates {
    Const(String),
    Paths(Vec<String>),
}

#[derive(Deserialize, Debug, Copy, Clone)]
#[serde(untagged)]
pub(super) enum BackendErrorId {
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
pub(super) enum RequestAccessMode {
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
pub(super) struct ExecuteQueryConfig {
    database: Option<String>,
    routing: Option<RequestAccessMode>,
    impersonated_user: Option<String>,
    bookmark_manager_id: Option<ExecuteQueryBmmId>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub(super) enum ExecuteQueryBmmId {
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
    pub(super) fn handle(self, backend: &Backend) -> TestKitResult {
        match self {
            Request::StartTest { test_name } => backend.send(&Response::run_test(test_name))?,
            Request::StartSubTest {
                test_name,
                arguments,
            } => backend.send(&Response::run_sub_test(test_name, arguments)?)?,
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
            Request::SessionReadTransaction { .. } => self.session_read_transaction(backend)?,
            Request::SessionWriteTransaction { .. } => self.session_write_transaction(backend)?,
            Request::SessionBeginTransaction { .. } => self.session_begin_transaction(backend)?,
            Request::SessionLastBookmarks { .. } => self.session_last_bookmarks(backend)?,
            Request::TransactionRun { .. } => self.transaction_run(backend)?,
            Request::TransactionCommit { .. } => self.transaction_commit(backend)?,
            Request::TransactionRollback { .. } => self.transaction_rollback(backend)?,
            Request::TransactionClose { .. } => self.transaction_close(backend)?,
            Request::ResultNext { .. } => self.result_next(backend)?,
            Request::ResultSingle { .. } => self.result_single(backend)?,
            // Request::ResultSingleOptional { .. } => {},
            // Request::ResultPeek { .. } => {},
            Request::ResultConsume { .. } => self.result_consume(backend)?,
            // Request::ResultList { .. } => {},
            Request::RetryablePositive { .. } => self.retryable_positive(backend)?,
            Request::RetryableNegative { .. } => self.retryable_negative(backend)?,
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

    fn new_driver(self, backend: &Backend) -> TestKitResult {
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
        } = self
        else {
            panic!("expected Request::NewDriver");
        };
        let mut connection_config: ConnectionConfig = uri.as_str().try_into()?;
        let mut driver_config = DriverConfig::new();
        let mut emulated_config = EmulatedDriverConfig::default();
        driver_config = set_auth(driver_config, auth)?;
        if let Some(user_agent) = user_agent {
            driver_config = driver_config.with_user_agent(user_agent);
        }
        if auth_token_manager_id.is_some() {
            return Err(TestKitError::backend_err("auth token manager unsupported"));
        }
        let resolver_registered = resolver_registered.unwrap_or_default();
        let dns_registered = dns_registered.unwrap_or_default();
        if resolver_registered || dns_registered {
            let resolver = TestKitResolver::new(
                Arc::clone(&backend.io),
                backend.id_generator.clone(),
                resolver_registered,
                dns_registered,
            );
            driver_config = driver_config.with_resolver(Box::new(resolver));
        }
        if let Some(connection_timeout_ms) = connection_timeout_ms {
            driver_config =
                driver_config.with_connection_timeout(Duration::from_millis(connection_timeout_ms));
        }
        if let Some(fetch_size) = fetch_size {
            if fetch_size == -1 {
                driver_config = driver_config.with_fetch_all();
            } else if fetch_size > 0 {
                driver_config = driver_config.with_fetch_size(fetch_size as u64).unwrap();
            } else {
                return Err(TestKitError::backend_err(
                    "fetch size must be positive or -1",
                ));
            }
        }
        if let Some(timeout) = max_tx_retry_time_ms {
            emulated_config = emulated_config.with_max_retry_time(Duration::from_millis(timeout));
        }
        if liveness_check_timeout_ms.is_some() {
            return Err(TestKitError::backend_err("liveness check unsupported"));
        }
        if let Some(max_connection_pool_size) = max_connection_pool_size {
            driver_config = driver_config.with_max_connection_pool_size(max_connection_pool_size);
        }
        if let Some(connection_acquisition_timeout_ms) = connection_acquisition_timeout_ms {
            driver_config = driver_config.with_connection_acquisition_timeout(
                Duration::from_millis(connection_acquisition_timeout_ms),
            );
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
        if let Some(encrypted) = encrypted {
            connection_config = match encrypted {
                true => match trusted_certificates {
                    None => connection_config.with_encryption_trust_default_cas()?,
                    Some(mut cas) => {
                        cas = cas
                            .into_iter()
                            .map(|name| format!("/usr/local/share/custom-ca-certificates/{name}"))
                            .collect::<Vec<_>>();
                        match cas.is_empty() {
                            true => connection_config.with_encryption_trust_any_certificate()?,
                            false => connection_config.with_encryption_trust_custom_cas(&cas)?,
                        }
                    }
                },
                false => connection_config.with_encryption_disabled(),
            }
        }
        let id = backend.next_id();
        let driver_holder = DriverHolder::new(
            &id,
            backend.id_generator.clone(),
            connection_config,
            driver_config,
            emulated_config,
        );
        backend
            .data
            .borrow_mut()
            .drivers
            .insert(id, Some(driver_holder));
        backend.send(&Response::Driver { id })
    }

    fn driver_close(self, backend: &Backend) -> TestKitResult {
        let Request::DriverClose { driver_id } = self else {
            panic!("expected Request::DriverClose");
        };
        let mut data = backend.data.borrow_mut();
        let Some(driver_holder) = data.drivers.get_mut(&driver_id) else {
            return Err(missing_driver_error(&driver_id));
        };
        let Some(driver_holder) = driver_holder.take() else {
            return Err(closed_driver_error());
        };
        drop(driver_holder);
        backend.send(&Response::Driver { id: driver_id })
    }

    fn new_session(self, backend: &Backend) -> TestKitResult {
        let Request::NewSession {
            driver_id,
            access_mode,
            bookmarks,
            database,
            fetch_size,
            impersonated_user,
            notifications_min_severity,
            notifications_disabled_categories,
            bookmark_manager_id,
        } = self
        else {
            panic!("expected Request::NewDriver");
        };
        let mut data = backend.data.borrow_mut();
        let driver_holder = get_driver(&data, &driver_id)?;
        let mut config = SessionConfig::new();
        if let Some(bookmarks) = bookmarks {
            config = config.with_bookmarks(Bookmarks::from_raw(bookmarks));
        }
        if let Some(database) = database {
            config = config.with_database(database);
        }
        if let Some(fetch_size) = fetch_size {
            if fetch_size == -1 {
                config = config.with_fetch_all();
            } else if fetch_size > 0 {
                config = config.with_fetch_size(fetch_size as u64).unwrap();
            } else {
                return Err(TestKitError::backend_err(
                    "fetch size must be positive or -1",
                ));
            }
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
        data.session_id_to_driver_id.insert(id, Some(driver_id));
        backend.send(&Response::Session { id })
    }

    fn close_session(self, backend: &Backend) -> TestKitResult {
        let Request::SessionClose { session_id } = self else {
            panic!("expected Request::SessionClose");
        };
        let mut data = backend.data.borrow_mut();
        let Some(driver_id) = data.session_id_to_driver_id.get_mut(&session_id) else {
            return Err(missing_session_error(&session_id));
        };
        driver_id
            .take()
            .and_then(|driver_id| data.drivers.get(&driver_id))
            .unwrap()
            .as_ref()
            .map(|driver| driver.session_close(CloseSession { session_id }).result)
            .transpose()?;
        backend.send(&Response::Session { id: session_id })
    }

    fn session_auto_commit(self, backend: &Backend) -> TestKitResult {
        let Request::SessionRun {
            session_id,
            query,
            params,
            tx_meta,
            timeout,
        } = self
        else {
            panic!("expected Request::SessionRun");
        };
        let mut data = backend.data.borrow_mut();
        let (driver_holder, driver_id) = get_driver_for_session(&data, &session_id)?;
        let params = params.map(cypher_value_map_to_value_send_map).transpose()?;
        let tx_meta = tx_meta
            .map(cypher_value_map_to_value_send_map)
            .transpose()?;
        let (result_id, keys) = driver_holder
            .auto_commit(AutoCommit {
                session_id,
                query,
                params,
                tx_meta,
                timeout,
            })
            .result?;
        data.result_id_to_driver_id.insert(result_id, driver_id);
        backend.send(&Response::Result {
            id: result_id,
            keys: keys.into_iter().map(|k| (*k).clone()).collect(),
        })
    }

    fn session_read_transaction(self, backend: &Backend) -> TestKitResult {
        let Request::SessionReadTransaction {
            session_id,
            tx_meta,
            timeout,
        } = self
        else {
            panic!("expected Request::SessionReadTransaction");
        };
        let mut data = backend.data.borrow_mut();
        let (driver_holder, driver_id) = get_driver_for_session(&data, &session_id)?;
        let tx_meta = tx_meta
            .map(cypher_value_map_to_value_send_map)
            .transpose()?;
        let retry_outcome = driver_holder
            .transaction_function(TransactionFunction {
                session_id,
                tx_meta,
                timeout,
                access_mode: RoutingControl::Read,
            })
            .result?;
        let msg = handle_retry_outcome(&mut data, retry_outcome, driver_id);
        backend.send(&msg)
    }

    fn session_write_transaction(self, backend: &Backend) -> TestKitResult {
        let Request::SessionWriteTransaction {
            session_id,
            tx_meta,
            timeout,
        } = self
        else {
            panic!("expected Request::SessionWriteTransaction");
        };
        let mut data = backend.data.borrow_mut();
        let (driver_holder, driver_id) = get_driver_for_session(&data, &session_id)?;
        let tx_meta = tx_meta
            .map(cypher_value_map_to_value_send_map)
            .transpose()?;
        let retry_outcome = driver_holder
            .transaction_function(TransactionFunction {
                session_id,
                tx_meta,
                timeout,
                access_mode: RoutingControl::Write,
            })
            .result?;
        let msg = handle_retry_outcome(&mut data, retry_outcome, driver_id);
        backend.send(&msg)
    }

    fn session_begin_transaction(self, backend: &Backend) -> TestKitResult {
        let Request::SessionBeginTransaction {
            session_id,
            tx_meta,
            timeout,
        } = self
        else {
            panic!("expected Request::SessionBeginTransaction");
        };
        let mut data = backend.data.borrow_mut();
        let (driver_holder, driver_id) = get_driver_for_session(&data, &session_id)?;
        let tx_meta = tx_meta
            .map(cypher_value_map_to_value_send_map)
            .transpose()?;
        let tx_id = driver_holder
            .begin_transaction(BeginTransaction {
                session_id,
                tx_meta,
                timeout,
            })
            .result?;
        data.tx_id_to_driver_id.insert(tx_id, driver_id);
        backend.send(&Response::Transaction { id: tx_id })
    }

    fn session_last_bookmarks(self, backend: &Backend) -> TestKitResult {
        let Request::SessionLastBookmarks { session_id } = self else {
            panic!("expected Request::SessionLastBookmarks");
        };
        let data = backend.data.borrow();
        let (driver_holder, _) = get_driver_for_session(&data, &session_id)?;
        let bookmarks = driver_holder
            .last_bookmarks(LastBookmarks { session_id })
            .result?;
        backend.send(&Response::Bookmarks {
            bookmarks: bookmarks.into_raw().collect(),
        })
    }

    fn transaction_run(self, backend: &Backend) -> TestKitResult {
        let Request::TransactionRun {
            transaction_id,
            query,
            params,
        } = self
        else {
            panic!("expected Request::TransactionRun");
        };
        let mut data = backend.data.borrow_mut();
        let Some(&driver_id) = data.tx_id_to_driver_id.get(&transaction_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown transaction id {} in backend",
                transaction_id
            )));
        };
        let params = params.map(cypher_value_map_to_value_send_map).transpose()?;
        let (result_id, keys) = get_driver(&data, &driver_id)?
            .transaction_run(TransactionRun {
                transaction_id,
                query,
                params,
            })
            .result?;
        data.result_id_to_driver_id.insert(result_id, driver_id);
        backend.send(&Response::Result {
            id: result_id,
            keys: keys.into_iter().map(|k| (*k).clone()).collect(),
        })
    }

    fn transaction_commit(self, backend: &Backend) -> TestKitResult {
        let Request::TransactionCommit { transaction_id } = self else {
            panic!("expected Request::TransactionCommit");
        };
        let data = backend.data.borrow();
        let Some(&driver_id) = data.tx_id_to_driver_id.get(&transaction_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown transaction id {} in backend",
                transaction_id
            )));
        };
        get_driver(&data, &driver_id)?
            .commit_transaction(CommitTransaction { transaction_id })
            .result?;
        backend.send(&Response::Transaction { id: transaction_id })
    }

    fn transaction_rollback(self, backend: &Backend) -> TestKitResult {
        let Request::TransactionRollback { transaction_id } = self else {
            panic!("expected Request::TransactionRollback");
        };
        let data = backend.data.borrow();
        let Some(&driver_id) = data.tx_id_to_driver_id.get(&transaction_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown transaction id {} in backend",
                transaction_id
            )));
        };
        get_driver(&data, &driver_id)?
            .rollback_transaction(RollbackTransaction { transaction_id })
            .result??;
        backend.send(&Response::Transaction { id: transaction_id })
    }

    fn transaction_close(self, backend: &Backend) -> TestKitResult {
        let Request::TransactionClose { transaction_id } = self else {
            panic!("expected Request::TransactionClose");
        };
        let data = backend.data.borrow();
        let Some(&driver_id) = data.tx_id_to_driver_id.get(&transaction_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown transaction id {} in backend",
                transaction_id
            )));
        };
        get_driver(&data, &driver_id)?
            .close_transaction(CloseTransaction { transaction_id })
            .result??;
        backend.send(&Response::Transaction { id: transaction_id })
    }

    fn result_next(self, backend: &Backend) -> TestKitResult {
        let Request::ResultNext { result_id } = self else {
            panic!("expected Request::ResultNext");
        };
        let data = backend.data.borrow();
        let Some(&driver_id) = data.result_id_to_driver_id.get(&result_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown result id {result_id} in backend"
            )));
        };
        let record = get_driver(&data, &driver_id)?
            .result_next(ResultNext { result_id })
            .result?;
        let response = write_record(record)?;
        backend.send(&response)
    }

    fn result_single(self, backend: &Backend) -> TestKitResult {
        let Request::ResultSingle { result_id } = self else {
            panic!("expected Request::ResultSingle");
        };
        let data = backend.data.borrow();
        let Some(&driver_id) = data.result_id_to_driver_id.get(&result_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown result id {result_id} in backend"
            )));
        };
        let record = get_driver(&data, &driver_id)?
            .result_single(ResultSingle { result_id })
            .result?;
        let response = write_record(Some(record))?;
        backend.send(&response)
    }

    fn result_consume(self, backend: &Backend) -> TestKitResult {
        let Request::ResultConsume { result_id } = self else {
            panic!("expected Request::ResultConsume");
        };
        let mut data = backend.data.borrow_mut();
        if let Some(summary) = data.summaries.get(&result_id) {
            return backend.send(&Response::Summary(summary.clone().try_into()?));
        }
        let Some(&driver_id) = data.result_id_to_driver_id.get(&result_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown result id {result_id} in backend"
            )));
        };
        let summary = get_driver(&data, &driver_id)?
            .result_consume(ResultConsume { result_id })
            .result?;
        data.summaries.insert(result_id, summary.clone());
        backend.send(&Response::Summary(summary.try_into()?))
    }

    fn retryable_positive(self, backend: &Backend) -> TestKitResult {
        let Request::RetryablePositive { session_id } = self else {
            panic!("expected Request::RetryablePositive");
        };
        let mut data = backend.data.borrow_mut();
        let (driver_holder, driver_id) = get_driver_for_session(&data, &session_id)?;
        let retry_outcome = driver_holder
            .retryable_positive(RetryablePositive { session_id })
            .result?;
        let msg = handle_retry_outcome(&mut data, retry_outcome, driver_id);
        backend.send(&msg)
    }

    fn retryable_negative(self, backend: &Backend) -> TestKitResult {
        let Request::RetryableNegative {
            session_id,
            error_id,
        } = self
        else {
            panic!("expected Request::RetryableNegative");
        };
        let mut data = backend.data.borrow_mut();
        let (driver_holder, driver_id) = get_driver_for_session(&data, &session_id)?;
        let retry_outcome = driver_holder
            .retryable_negative(RetryableNegative {
                session_id,
                error_id,
            })
            .result?;
        let msg = handle_retry_outcome(&mut data, retry_outcome, driver_id);
        backend.send(&msg)
    }
}

fn handle_retry_outcome(
    backend_data: &mut BackendData,
    outcome: RetryableOutcome,
    driver_id: BackendId,
) -> Response {
    match outcome {
        RetryableOutcome::Retry(tx_id) => {
            backend_data.tx_id_to_driver_id.insert(tx_id, driver_id);
            Response::RetryableTry { id: tx_id }
        }
        RetryableOutcome::Done => Response::RetryableDone,
    }
}

fn get_driver<'a>(
    backend_data: &'a BackendData,
    driver_id: &BackendId,
) -> Result<&'a DriverHolder, TestKitError> {
    backend_data
        .drivers
        .get(driver_id)
        .ok_or_else(|| missing_driver_error(driver_id))?
        .as_ref()
        .ok_or_else(closed_driver_error)
}

fn get_driver_for_session<'a>(
    backend_data: &'a BackendData,
    session_id: &BackendId,
) -> Result<(&'a DriverHolder, BackendId), TestKitError> {
    let driver_id = backend_data
        .session_id_to_driver_id
        .get(session_id)
        .ok_or_else(|| missing_session_error(session_id))?
        .as_ref()
        .ok_or_else(closed_session_error)?;
    backend_data
        .drivers
        .get(driver_id)
        .unwrap()
        .as_ref()
        .ok_or_else(closed_driver_error)
        .map(|driver| (driver, *driver_id))
}

fn closed_driver_error() -> TestKitError {
    TestKitError::DriverError {
        error_type: String::from("DriverClosed"),
        msg: String::from("The driver has been closed"),
        code: None,
        id: None,
    }
}

fn missing_driver_error(driver_id: &BackendId) -> TestKitError {
    TestKitError::backend_err(format!("No driver with id {driver_id} found"))
}

fn closed_session_error() -> TestKitError {
    TestKitError::DriverError {
        error_type: String::from("SessionClosed"),
        msg: String::from("The session  has been closed"),
        code: None,
        id: None,
    }
}

fn missing_session_error(session_id: &BackendId) -> TestKitError {
    TestKitError::backend_err(format!("No session with id {session_id} found"))
}

fn set_auth(mut config: DriverConfig, auth: TestKitAuth) -> Result<DriverConfig, TestKitError> {
    let TestKitAuth::AuthorizationToken { scheme, mut data } = auth;
    match scheme.as_str() {
        "basic" => {
            let Value::String(principal) = data.remove("principal").ok_or_else(|| {
                TestKitError::backend_err("auth: basic scheme required principal")
            })?
            else {
                return Err(TestKitError::backend_err(
                    "auth: principal needs to be string",
                ));
            };
            let Value::String(credentials) = data.remove("credentials").ok_or_else(|| {
                TestKitError::backend_err("auth: basic scheme required credentials")
            })?
            else {
                return Err(TestKitError::backend_err(
                    "auth: credentials needs to be string",
                ));
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

fn cypher_value_map_to_value_send_map(
    map: HashMap<String, CypherValue>,
) -> Result<HashMap<String, ValueSend>, TestKitError> {
    map.into_iter()
        .map(|(k, v)| Ok::<_, TestKitError>((k, v.try_into()?)))
        .collect::<Result<_, _>>()
}

fn write_record(values: Option<Vec<CypherValue>>) -> Result<Response, TestKitError> {
    let response = match values {
        None => Response::NullRecord,
        Some(values) => Response::Record { values },
    };
    Ok(response)
}
