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
use std::sync::Arc;
use std::time::Duration;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;

use neo4j::bookmarks::{BookmarkManager, Bookmarks};
use neo4j::driver::auth::AuthToken;
use neo4j::driver::notification::{
    DisabledCategory, DisabledClassification, MinimumSeverity, NotificationFilter,
};
use neo4j::driver::{ConnectionConfig, DriverConfig, RoutingControl};
use neo4j::session::SessionConfig;
use neo4j::transaction::TransactionTimeout;
use neo4j::ValueSend;

use super::auth::TestKitAuthManagers;
use super::bookmarks::new_bookmark_manager;
use super::cypher_value::{ConvertableJsonValue, ConvertableValueSend, CypherValue, CypherValues};
use super::driver_holder::{
    CloseSession, DriverHolder, EmulatedDriverConfig, ExecuteQuery, ExecuteQueryBookmarkManager,
    GetConnectionPoolMetrics, NewSession, VerifyAuthentication,
};
use super::errors::TestKitError;
use super::resolver::TestKitResolver;
use super::responses::Response;
use super::session_holder::{
    AutoCommit, BeginTransaction, CloseTransaction, CommitTransaction, LastBookmarks,
    ResultConsume, ResultNext, ResultSingle, RetryableNegative, RetryableOutcome,
    RetryablePositive, RollbackTransaction, TransactionFunction, TransactionRun,
};
use super::{Backend, BackendData, BackendId, TestKitResult, TestKitResultT};

#[allow(dead_code)] // reflects TestKit protocol
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
        arguments: HashMap<String, JsonValue>,
    },
    GetFeatures {},
    #[serde(rename_all = "camelCase")]
    NewDriver {
        uri: String,
        #[serde(rename = "authorizationToken")]
        auth: MaybeTestKitAuth,
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
        #[serde(rename = "clientCertificate")]
        client_certificate: Option<ClientCertificate>,
        #[serde(rename = "clientCertificateProviderId")]
        client_certificate_provider_id: Option<BackendId>,
        notifications_min_severity: Option<String>,
        notifications_disabled_categories: Option<Vec<String>>,
        notifications_disabled_classifications: Option<Vec<String>>,
        #[serde(rename = "telemetryDisabled")]
        telemetry_disabled: Option<bool>,
        encrypted: Option<bool>,
        trusted_certificates: Option<Vec<String>>,
    },
    NewAuthTokenManager {},
    #[serde(rename_all = "camelCase")]
    AuthTokenManagerGetAuthCompleted {
        request_id: BackendId,
        auth: TestKitAuth,
    },
    #[serde(rename_all = "camelCase")]
    AuthTokenManagerHandleSecurityExceptionCompleted {
        request_id: BackendId,
        handled: bool,
    },
    AuthTokenManagerClose {
        id: BackendId,
    },
    NewBasicAuthTokenManager {},
    #[serde(rename_all = "camelCase")]
    BasicAuthTokenProviderCompleted {
        request_id: BackendId,
        auth: TestKitAuth,
    },
    NewBearerAuthTokenManager {},
    #[serde(rename_all = "camelCase")]
    BearerAuthTokenProviderCompleted {
        request_id: BackendId,
        auth: AuthTokenAndExpiration,
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
    VerifyAuthentication {
        driver_id: BackendId,
        #[serde(rename = "authorizationToken")]
        auth: MaybeTestKitAuth,
    },
    #[serde(rename_all = "camelCase")]
    CheckSessionAuthSupport {
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
        request_id: BackendId,
        bookmarks: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    BookmarksConsumerCompleted {
        request_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    NewBookmarkManager {
        initial_bookmarks: Option<Vec<String>>,
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
        fetch_size: Option<i64>,
        impersonated_user: Option<String>,
        notifications_min_severity: Option<String>,
        notifications_disabled_categories: Option<Vec<String>>,
        notifications_disabled_classifications: Option<Vec<String>>,
        bookmark_manager_id: Option<BackendId>,
        #[serde(rename = "authorizationToken")]
        auth: MaybeTestKitAuth,
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
        params: Option<HashMap<String, CypherValue>>,
        config: ExecuteQueryConfig,
    },
    FakeTimeInstall {},
    #[serde(rename_all = "camelCase")]
    FakeTimeTick {
        increment_ms: i64,
    },
    FakeTimeUninstall {},
}

#[derive(Deserialize, Serialize, Debug)]
#[repr(transparent)]
#[serde(transparent)]
pub(super) struct MaybeTestKitAuth(pub(super) Option<TestKitAuth>);

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(super) enum TestKitAuth {
    AuthorizationToken(AuthTokenData),
}

impl From<TestKitAuth> for AuthToken {
    fn from(value: TestKitAuth) -> Self {
        let TestKitAuth::AuthorizationToken(data) = value;
        match data {
            AuthTokenData::Basic {
                principal,
                credentials,
                realm,
            } => match realm {
                None => AuthToken::new_basic_auth(principal, credentials),
                Some(realm) => AuthToken::new_basic_auth_with_realm(principal, credentials, realm),
            },
            AuthTokenData::Kerberos { credentials } => AuthToken::new_kerberos_auth(credentials),
            AuthTokenData::Bearer { credentials } => AuthToken::new_bearer_auth(credentials),
            AuthTokenData::Custom {
                scheme,
                principal,
                credentials,
                realm,
                parameters,
            } => {
                let parameters = parameters.map(|p| {
                    p.into_iter()
                        .map(|(k, v)| (k, ConvertableJsonValue(v).into()))
                        .collect::<HashMap<String, ValueSend>>()
                });
                AuthToken::new_custom_auth(principal, credentials, realm, Some(scheme), parameters)
            }
        }
    }
}

impl From<MaybeTestKitAuth> for AuthToken {
    fn from(value: MaybeTestKitAuth) -> Self {
        match value.0 {
            None => AuthToken::new_none_auth(),
            Some(value) => value.into(),
        }
    }
}

impl TestKitAuth {
    pub(super) fn try_clone_auth_token(auth: &AuthToken) -> TestKitResultT<Self> {
        let mut auth = auth.data().clone();
        let scheme = auth
            .remove("scheme")
            .ok_or_else(|| {
                TestKitError::backend_err("cannot serialize AuthToken without \"scheme\"")
            })?
            .try_into_string()
            .map_err(|err| {
                TestKitError::backend_err(format!(
                    "cannot serialize AuthToken with non-string \"scheme\": {err:?}"
                ))
            })?;
        let credentials = auth
            .remove("credentials")
            .map(|credentials| {
                credentials.try_into_string().map_err(|err| {
                    TestKitError::backend_err(format!(
                        "cannot serialize AuthToken with non-string \"credentials\": {err:?}"
                    ))
                })
            })
            .transpose()?;
        let auth_token = match scheme.as_str() {
            "basic" => {
                let credentials = credentials.ok_or_else(|| {
                    TestKitError::backend_err(
                        "cannot serialize basic AuthToken without \"credentials\"",
                    )
                })?;
                let principal = auth
                    .remove("principal")
                    .ok_or_else(|| {
                        TestKitError::backend_err("cannot serialize basic AuthToken without \"principal\"")
                    })?
                    .try_into_string()
                    .map_err(|err| {
                        TestKitError::backend_err(format!(
                            "cannot serialize basic AuthToken with non-string \"principal\": {err:?}"
                        ))
                    })?;
                let realm = auth
                    .remove("realm")
                    .map(|realm| {
                        realm.try_into_string().map_err(|err| {
                            TestKitError::backend_err(format!(
                                "cannot serialize basic AuthToken with non-string \"realm\": {err:?}"
                            ))
                        })
                    })
                    .transpose()?;
                TestKitAuth::AuthorizationToken(AuthTokenData::Basic {
                    principal,
                    credentials,
                    realm,
                })
            }
            "kerberos" => {
                let credentials = credentials.ok_or_else(|| {
                    TestKitError::backend_err(
                        "cannot serialize kerberos AuthToken without \"credentials\"",
                    )
                })?;
                TestKitAuth::AuthorizationToken(AuthTokenData::Kerberos { credentials })
            }
            "bearer" => {
                let credentials = credentials.ok_or_else(|| {
                    TestKitError::backend_err(
                        "cannot serialize bearer AuthToken without \"credentials\"",
                    )
                })?;
                TestKitAuth::AuthorizationToken(AuthTokenData::Bearer { credentials })
            }
            _ => {
                let principal = auth
                    .remove("principal")
                    .map(|principal| {
                        principal.try_into_string().map_err(|err| {
                            TestKitError::backend_err(format!(
                                "cannot serialize custom AuthToken with non-string \"principal\": {err:?}"
                            ))
                        })
                    })
                    .transpose()?;
                let realm = auth
                    .remove("realm")
                    .map(|realm| {
                        realm.try_into_string().map_err(|err| {
                            TestKitError::backend_err(format!(
                                "cannot serialize custom AuthToken with non-string \"realm\": {err:?}"
                            ))
                        })
                    })
                    .transpose()?;
                let parameters = auth
                    .remove("parameters")
                    .map(|parameters| {
                        parameters
                            .try_into_map()
                            .map_err(|err| {
                                TestKitError::backend_err(format!(
                                    "cannot serialize custom AuthToken with non-map \"parameters\": {err:?}"
                                ))
                            })
                            .and_then(|parameters| {
                                parameters
                                    .into_iter()
                                    .map(|(k, v)| Ok((k, ConvertableValueSend(v).try_into()?)))
                                    .collect::<Result<HashMap<String, JsonValue>, String>>()
                                    .map_err(TestKitError::backend_err)
                            })
                    })
                    .transpose()?;
                TestKitAuth::AuthorizationToken(AuthTokenData::Custom {
                    scheme,
                    principal,
                    credentials,
                    realm,
                    parameters,
                })
            }
        };
        if !auth.is_empty() {
            return Err(TestKitError::backend_err(format!(
                "cannot serialize AuthToken with unknown fields: {fields:?}",
                fields = auth.keys().collect::<Vec<_>>()
            )));
        }
        Ok(auth_token)
    }
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "scheme")]
pub(super) enum AuthTokenData {
    #[serde(rename = "basic")]
    Basic {
        principal: String,
        credentials: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        realm: Option<String>,
    },
    #[serde(rename = "kerberos")]
    Kerberos { credentials: String },
    #[serde(rename = "bearer")]
    Bearer { credentials: String },
    #[serde(untagged)]
    Custom {
        scheme: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        principal: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        credentials: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        realm: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parameters: Option<HashMap<String, JsonValue>>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(super) enum AuthTokenAndExpiration {
    #[serde(rename_all = "camelCase")]
    AuthTokenAndExpiration {
        auth: TestKitAuth,
        expires_in_ms: Option<i64>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(tag = "name", content = "data")]
#[allow(dead_code)] // reflects TestKit protocol
pub(super) enum ClientCertificate {
    ClientCertificate {
        certfile: String,
        keyfile: String,
        password: Option<String>,
    },
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
#[allow(dead_code)] // reflects TestKit protocol
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
    tx_meta: Option<HashMap<String, CypherValue>>,
    timeout: Option<i64>,
    #[serde(rename = "authorizationToken")]
    auth: MaybeTestKitAuth,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub(super) enum ExecuteQueryBmmId {
    BackendId(BackendId),
    #[serde(deserialize_with = "deserialize_default_bmm_id")]
    None,
}

fn deserialize_default_bmm_id<'de, D>(d: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    match i8::deserialize(d)? {
        -1 => Ok(()),
        _ => Err(D::Error::custom("None BMM ID must be -1")),
    }
}

fn load_notification_filter(
    notifications_min_severity: Option<String>,
    notifications_disabled_categories: Option<Vec<String>>,
    notifications_disabled_classifications: Option<Vec<String>>,
) -> TestKitResultT<Option<NotificationFilter>> {
    if notifications_disabled_categories.is_none()
        && notifications_min_severity.is_none()
        && notifications_disabled_classifications.is_none()
    {
        return Ok(None);
    }
    let minimum_severity = match notifications_min_severity {
        None => None,
        Some(severity) => Some(match severity.as_str() {
            "OFF" => MinimumSeverity::Disabled,
            "INFORMATION" => MinimumSeverity::Information,
            "WARNING" => MinimumSeverity::Warning,
            _ => {
                return Err(TestKitError::backend_err(format!(
                    "unknown minimum notification severity: {severity:?}",
                    severity = severity
                )))
            }
        }),
    };
    let disabled_categories = notifications_disabled_categories
        .map(|categories| {
            categories
                .into_iter()
                .map(|category| {
                    Ok(match category.as_str() {
                        "HINT" => DisabledCategory::Hint,
                        "UNRECOGNIZED" => DisabledCategory::Unrecognized,
                        "UNSUPPORTED" => DisabledCategory::Unsupported,
                        "PERFORMANCE" => DisabledCategory::Performance,
                        "DEPRECATION" => DisabledCategory::Deprecation,
                        "GENERIC" => DisabledCategory::Generic,
                        "SECURITY" => DisabledCategory::Security,
                        "TOPOLOGY" => DisabledCategory::Topology,
                        "SCHEMA" => DisabledCategory::Schema,
                        _ => {
                            return Err(TestKitError::backend_err(format!(
                                "unknown disabled notification category: {cat:?}",
                                cat = category
                            )))
                        }
                    })
                })
                .collect::<Result<_, _>>()
        })
        .transpose()?;
    let disabled_classifications = notifications_disabled_classifications
        .map(|classifications| {
            classifications
                .into_iter()
                .map(|classification| {
                    Ok(match classification.as_str() {
                        "HINT" => DisabledClassification::Hint,
                        "UNRECOGNIZED" => DisabledClassification::Unrecognized,
                        "UNSUPPORTED" => DisabledClassification::Unsupported,
                        "PERFORMANCE" => DisabledClassification::Performance,
                        "DEPRECATION" => DisabledClassification::Deprecation,
                        "GENERIC" => DisabledClassification::Generic,
                        "SECURITY" => DisabledClassification::Security,
                        "TOPOLOGY" => DisabledClassification::Topology,
                        "SCHEMA" => DisabledClassification::Schema,
                        _ => {
                            return Err(TestKitError::backend_err(format!(
                                "unknown disabled notification classification: {cls:?}",
                                cls = classification
                            )))
                        }
                    })
                })
                .collect::<Result<_, _>>()
        })
        .transpose()?;
    let filter = NotificationFilter::new();
    let filter = match minimum_severity {
        None => filter,
        Some(minimum_severity) => filter.with_minimum_severity(minimum_severity),
    };
    let filter = match disabled_categories {
        None => filter,
        Some(disabled_categories) => filter.with_disabled_categories(disabled_categories),
    };
    let filter = match disabled_classifications {
        None => filter,
        Some(disabled_classifications) => {
            filter.with_disabled_classifications(disabled_classifications)
        }
    };
    Ok(Some(filter))
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
            Request::NewAuthTokenManager { .. } => self.new_auth_token_manager(backend)?,
            Request::AuthTokenManagerGetAuthCompleted { .. } => {
                return self.unexpected_resolution()
            }
            Request::AuthTokenManagerHandleSecurityExceptionCompleted { .. } => {
                return self.unexpected_resolution()
            }
            Request::AuthTokenManagerClose { .. } => self.auth_token_manager_close(backend)?,
            Request::NewBasicAuthTokenManager { .. } => {
                self.new_basic_auth_token_manager(backend)?
            }
            Request::BasicAuthTokenProviderCompleted { .. } => return self.unexpected_resolution(),
            Request::NewBearerAuthTokenManager { .. } => {
                self.new_bearer_auth_token_manager(backend)?
            }
            Request::BearerAuthTokenProviderCompleted { .. } => {
                return self.unexpected_resolution()
            }
            Request::VerifyConnectivity { .. } => self.verify_connectivity(backend)?,
            Request::GetServerInfo { .. } => self.get_server_info(backend)?,
            Request::CheckMultiDBSupport { .. } => self.check_multi_db_support(backend)?,
            Request::VerifyAuthentication { .. } => self.verify_authentication(backend)?,
            Request::CheckSessionAuthSupport { .. } => self.check_session_auth_support(backend)?,
            Request::CheckDriverIsEncrypted { .. } => self.check_driver_is_encrypted(backend)?,
            Request::ResolverResolutionCompleted { .. } => return self.unexpected_resolution(),
            Request::BookmarksSupplierCompleted { .. } => return self.unexpected_resolution(),
            Request::BookmarksConsumerCompleted { .. } => return self.unexpected_resolution(),
            Request::NewBookmarkManager { .. } => self.new_bookmark_manager(backend)?,
            Request::BookmarkManagerClose { .. } => self.close_bookmark_manager(backend)?,
            Request::DomainNameResolutionCompleted { .. } => return self.unexpected_resolution(),
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
            Request::GetConnectionPoolMetrics { .. } => {
                self.get_connection_pool_metrics(backend)?
            }
            Request::ExecuteQuery { .. } => self.execute_query(backend)?,
            Request::FakeTimeInstall { .. } => self.fake_time_install(backend)?,
            Request::FakeTimeTick { .. } => self.fake_time_tick(backend)?,
            Request::FakeTimeUninstall { .. } => self.fake_time_uninstall(backend)?,
            _ => {
                return Err(TestKitError::backend_err(format!(
                    "Unhandled request {:?}",
                    self
                )))
            }
        }
        Ok(())
    }

    fn unexpected_resolution(self) -> TestKitResult {
        Err(TestKitError::backend_err(format!(
            "Resolution {:?} not expected (the backend didn't ask for it).",
            self
        )))
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
            client_certificate,
            client_certificate_provider_id,
            notifications_min_severity,
            notifications_disabled_categories,
            notifications_disabled_classifications,
            telemetry_disabled,
            encrypted,
            trusted_certificates,
        } = self
        else {
            panic!("expected Request::NewDriver");
        };
        let mut connection_config: ConnectionConfig = uri.as_str().try_into()?;
        let mut driver_config = DriverConfig::new();
        let mut emulated_config = EmulatedDriverConfig::default();
        if let Some(auth) = auth.0 {
            driver_config = driver_config.with_auth(Arc::new(auth.into()));
        } else if let Some(auth_token_manager_id) = auth_token_manager_id {
            let data = backend.data.borrow();
            let auth_token_manager = data
                .auth_managers
                .get(&auth_token_manager_id)
                .ok_or_else(|| missing_auth_token_manager_error(&auth_token_manager_id))?;
            driver_config = driver_config.with_auth_manager(Arc::clone(auth_token_manager));
        } else {
            return Err(TestKitError::backend_err(
                "neither auth token nor manager present",
            ));
        }
        if let Some(user_agent) = user_agent {
            driver_config = driver_config.with_user_agent(user_agent);
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
        if let Some(timeout) = liveness_check_timeout_ms {
            driver_config =
                driver_config.with_idle_time_before_connection_test(Duration::from_millis(timeout));
        }
        if let Some(max_connection_pool_size) = max_connection_pool_size {
            driver_config = driver_config.with_max_connection_pool_size(max_connection_pool_size);
        }
        if let Some(connection_acquisition_timeout_ms) = connection_acquisition_timeout_ms {
            driver_config = driver_config.with_connection_acquisition_timeout(
                Duration::from_millis(connection_acquisition_timeout_ms),
            );
        }
        if client_certificate.is_some() || client_certificate_provider_id.is_some() {
            return Err(TestKitError::backend_err("mTLS not (yet) supported"));
        }
        if let Some(filter) = load_notification_filter(
            notifications_min_severity,
            notifications_disabled_categories,
            notifications_disabled_classifications,
        )? {
            driver_config = driver_config.with_notification_filter(filter);
        }
        if let Some(telemetry_disabled) = telemetry_disabled {
            driver_config = driver_config.with_telemetry(!telemetry_disabled);
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
                            true => connection_config.with_encryption_trust_any_certificate(),
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

    fn new_auth_token_manager(self, backend: &Backend) -> TestKitResult {
        let Request::NewAuthTokenManager {} = self else {
            panic!("expected Request::NewAuthTokenManager");
        };
        let (id, auth_token_manager) =
            TestKitAuthManagers::new_custom(Arc::clone(&backend.io), backend.id_generator.clone());
        backend
            .data
            .borrow_mut()
            .auth_managers
            .insert(id, auth_token_manager);
        backend.send(&Response::AuthTokenManager { id })
    }

    fn auth_token_manager_close(self, backend: &Backend) -> TestKitResult {
        let Request::AuthTokenManagerClose { id } = self else {
            panic!("expected Request::AuthTokenManagerClose");
        };
        let mut data = backend.data.borrow_mut();
        let Some(_) = data.auth_managers.remove(&id) else {
            return Err(missing_auth_token_manager_error(&id));
        };
        backend.send(&Response::AuthTokenManager { id })
    }

    fn new_basic_auth_token_manager(self, backend: &Backend) -> TestKitResult {
        let Request::NewBasicAuthTokenManager {} = self else {
            panic!("expected Request::NewBasicAuthTokenManager");
        };
        let (id, auth_token_manager) =
            TestKitAuthManagers::new_basic(Arc::clone(&backend.io), backend.id_generator.clone());
        backend
            .data
            .borrow_mut()
            .auth_managers
            .insert(id, auth_token_manager);
        backend.send(&Response::BasicAuthTokenManager { id })
    }

    fn new_bearer_auth_token_manager(self, backend: &Backend) -> TestKitResult {
        let Request::NewBearerAuthTokenManager {} = self else {
            panic!("expected Request::NewBearerAuthTokenManager");
        };
        let (id, auth_token_manager) =
            TestKitAuthManagers::new_bearer(Arc::clone(&backend.io), backend.id_generator.clone());
        backend
            .data
            .borrow_mut()
            .auth_managers
            .insert(id, auth_token_manager);
        backend.send(&Response::BearerAuthTokenManager { id })
    }

    fn verify_connectivity(self, backend: &Backend) -> TestKitResult {
        let Request::VerifyConnectivity { driver_id } = self else {
            panic!("expected Request::VerifyConnectivity");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        driver_holder.verify_connectivity().result?;
        backend.send(&Response::Driver { id: driver_id })
    }

    fn get_server_info(self, backend: &Backend) -> TestKitResult {
        let Request::GetServerInfo { driver_id } = self else {
            panic!("expected Request::GetServerInfo");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        let server_info = driver_holder.get_server_info().result?;
        backend.send(&Response::ServerInfo(server_info.into()))
    }

    fn check_multi_db_support(self, backend: &Backend) -> TestKitResult {
        let Request::CheckMultiDBSupport { driver_id } = self else {
            panic!("expected Request::CheckMultiDBSupport");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        let available = driver_holder.supports_multi_db().result?;
        let id = backend.next_id();
        backend.send(&Response::MultiDBSupport { id, available })
    }

    fn verify_authentication(self, backend: &Backend) -> TestKitResult {
        let Request::VerifyAuthentication { driver_id, auth } = self else {
            panic!("expected Request::VerifyAuthentication");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        let authenticated = driver_holder
            .verify_authentication(VerifyAuthentication { auth: auth.into() })
            .result?;
        backend.send(&Response::DriverIsAuthenticated {
            id: driver_id,
            authenticated,
        })
    }

    fn check_session_auth_support(self, backend: &Backend) -> TestKitResult {
        let Request::CheckSessionAuthSupport { driver_id } = self else {
            panic!("expected Request::CheckMultiDBSupport");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        let available = driver_holder.supports_session_auth().result?;
        let id = backend.next_id();
        backend.send(&Response::SessionAuthSupport { id, available })
    }

    fn check_driver_is_encrypted(self, backend: &Backend) -> TestKitResult {
        let Request::CheckDriverIsEncrypted { driver_id } = self else {
            panic!("expected Request::CheckDriverIsEncrypted");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        let encrypted = driver_holder.is_encrypted().result;
        backend.send(&Response::DriverIsEncrypted { encrypted })
    }

    fn new_bookmark_manager(self, backend: &Backend) -> TestKitResult {
        let Request::NewBookmarkManager {
            initial_bookmarks,
            bookmarks_supplier_registered,
            bookmarks_consumer_registered,
        } = self
        else {
            panic!("expected Request::NewBookmarkManager");
        };
        let (id, manager) = new_bookmark_manager(
            initial_bookmarks,
            bookmarks_supplier_registered,
            bookmarks_consumer_registered,
            Arc::clone(&backend.io),
            backend.id_generator.clone(),
        );
        backend
            .data
            .borrow_mut()
            .bookmark_managers
            .insert(id, Arc::new(manager));
        backend.send(&Response::BookmarkManager { id })
    }

    fn close_bookmark_manager(self, backend: &Backend) -> TestKitResult {
        let Request::BookmarkManagerClose { id } = self else {
            panic!("expected Request::BookmarkManagerClose");
        };
        let mut data = backend.data.borrow_mut();
        let Some(_) = data.bookmark_managers.remove(&id) else {
            return Err(missing_bookmark_manager_error(&id));
        };
        backend.send(&Response::BookmarkManager { id })
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
            notifications_disabled_classifications,
            bookmark_manager_id,
            auth,
        } = self
        else {
            panic!("expected Request::NewDriver");
        };
        let mut data = backend.data.borrow_mut();
        let driver_holder = get_driver(&data, &driver_id)?;
        let mut config = SessionConfig::new();
        if let Some(bookmarks) = bookmarks {
            config = config.with_bookmarks(Arc::new(Bookmarks::from_raw(bookmarks)));
        }
        if let Some(database) = database {
            config = config.with_database(Arc::new(database));
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
            config = config.with_impersonated_user(Arc::new(imp_user));
        }
        if let Some(filter) = load_notification_filter(
            notifications_min_severity,
            notifications_disabled_categories,
            notifications_disabled_classifications,
        )? {
            config = config.with_notification_filter(filter);
        }
        let bookmark_manager = bookmark_manager_id
            .map(|id| get_bookmark_manager(&data, &id))
            .transpose()?;
        if let Some(bookmark_manager) = bookmark_manager {
            config = config.with_bookmark_manager(bookmark_manager);
        }
        if let Some(auth) = auth.0 {
            config = config.with_session_auth(Arc::new(auth.into()));
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
        let timeout = read_transaction_timeout(timeout)?;
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
        let timeout = read_transaction_timeout(timeout)?;
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
        let timeout = read_transaction_timeout(timeout)?;
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
        let timeout = read_transaction_timeout(timeout)?;
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
            bookmarks: bookmarks.raw().map(String::from).collect(),
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
                "Unknown transaction id {transaction_id} in backend",
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
                "Unknown transaction id {transaction_id} in backend",
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
                "Unknown transaction id {transaction_id} in backend",
            )));
        };
        get_driver(&data, &driver_id)?
            .rollback_transaction(RollbackTransaction { transaction_id })
            .result?;
        backend.send(&Response::Transaction { id: transaction_id })
    }

    fn transaction_close(self, backend: &Backend) -> TestKitResult {
        let Request::TransactionClose { transaction_id } = self else {
            panic!("expected Request::TransactionClose");
        };
        let data = backend.data.borrow();
        let Some(&driver_id) = data.tx_id_to_driver_id.get(&transaction_id) else {
            return Err(TestKitError::backend_err(format!(
                "Unknown transaction id {transaction_id} in backend",
            )));
        };
        get_driver(&data, &driver_id)?
            .close_transaction(CloseTransaction { transaction_id })
            .result?;
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

    fn get_connection_pool_metrics(self, backend: &Backend) -> TestKitResult {
        let Request::GetConnectionPoolMetrics { driver_id, address } = self else {
            panic!("expected Request::GetConnectionPoolMetrics");
        };
        let data = backend.data.borrow();
        let driver_holder = get_driver(&data, &driver_id)?;
        let metrics = driver_holder
            .get_connection_pool_metrics(GetConnectionPoolMetrics { address })
            .result?;
        backend.send(&Response::ConnectionPoolMetrics {
            in_use: metrics.in_use,
            idle: metrics.idle,
        })
    }

    fn execute_query(self, backend: &Backend) -> TestKitResult {
        let Request::ExecuteQuery {
            driver_id,
            query,
            params,
            config,
        } = self
        else {
            panic!("expected Request::ExecuteQuery");
        };
        let ExecuteQueryConfig {
            database,
            routing,
            impersonated_user,
            bookmark_manager_id,
            tx_meta,
            timeout,
            auth,
        } = config;
        let data = backend.data.borrow_mut();
        let driver_holder = get_driver(&data, &driver_id)?;
        let params = params.map(cypher_value_map_to_value_send_map).transpose()?;
        let routing = routing.map(Into::into);
        let bookmark_manager = match bookmark_manager_id {
            None => ExecuteQueryBookmarkManager::Default,
            Some(ExecuteQueryBmmId::BackendId(id)) => {
                let manager = get_bookmark_manager(&data, &id)?;
                ExecuteQueryBookmarkManager::Custom(manager)
            }
            Some(ExecuteQueryBmmId::None) => ExecuteQueryBookmarkManager::None,
        };
        let tx_meta = tx_meta
            .map(cypher_value_map_to_value_send_map)
            .transpose()?;
        let timeout = read_transaction_timeout(timeout)?;
        let auth = auth.0.map(|auth| Arc::new(auth.into()));
        let result = driver_holder
            .execute_query(ExecuteQuery {
                query,
                params,
                database,
                routing,
                impersonated_user,
                bookmark_manager,
                tx_meta,
                timeout,
                auth,
            })
            .result?;
        let response: Response = result.try_into()?;
        backend.send(&response)
    }

    fn fake_time_install(&self, backend: &Backend) -> TestKitResult {
        neo4j::time::freeze_time();
        let response = Response::FakeTimeAck;
        backend.send(&response)
    }

    fn fake_time_tick(&self, backend: &Backend) -> TestKitResult {
        let Request::FakeTimeTick { increment_ms } = self else {
            panic!("expected Request::FakeTimeTick");
        };
        let increment_ms = u64::try_from(*increment_ms)
            .map_err(|_| TestKitError::backend_err("increment_ms cannot be negative"))?;
        neo4j::time::tick(Duration::from_millis(increment_ms))
            .map_err(TestKitError::backend_err)?;
        let response = Response::FakeTimeAck;
        backend.send(&response)
    }

    fn fake_time_uninstall(&self, backend: &Backend) -> TestKitResult {
        neo4j::time::unfreeze_time().map_err(TestKitError::backend_err)?;
        let response = Response::FakeTimeAck;
        backend.send(&response)
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
) -> TestKitResultT<&'a DriverHolder> {
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
) -> TestKitResultT<(&'a DriverHolder, BackendId)> {
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
    TestKitError::driver_error_client_only(
        String::from("DriverClosed"),
        String::from("The driver has been closed"),
        false,
    )
}

fn missing_driver_error(driver_id: &BackendId) -> TestKitError {
    TestKitError::backend_err(format!("No driver with id {driver_id} found"))
}

fn missing_auth_token_manager_error(auth_token_manager_id: &BackendId) -> TestKitError {
    TestKitError::backend_err(format!(
        "No auth token manager with id {auth_token_manager_id} found"
    ))
}

fn missing_bookmark_manager_error(bookmark_manager_id: &BackendId) -> TestKitError {
    TestKitError::backend_err(format!(
        "No bookmark manager with id {bookmark_manager_id} found"
    ))
}

fn closed_session_error() -> TestKitError {
    TestKitError::driver_error_client_only(
        String::from("SessionClosed"),
        String::from("The session  has been closed"),
        false,
    )
}

fn get_bookmark_manager(
    backend_data: &BackendData,
    bookmark_manager_id: &BackendId,
) -> TestKitResultT<Arc<dyn BookmarkManager>> {
    backend_data
        .bookmark_managers
        .get(bookmark_manager_id)
        .ok_or_else(|| missing_bookmark_manager_error(bookmark_manager_id))
        .cloned()
}

fn missing_session_error(session_id: &BackendId) -> TestKitError {
    TestKitError::backend_err(format!("No session with id {session_id} found"))
}

fn cypher_value_map_to_value_send_map(
    map: HashMap<String, CypherValue>,
) -> TestKitResultT<HashMap<String, ValueSend>> {
    map.into_iter()
        .map(|(k, v)| Ok::<_, TestKitError>((k, v.try_into()?)))
        .collect::<Result<_, _>>()
}

fn write_record(values: Option<CypherValues>) -> TestKitResultT<Response> {
    let response = match values {
        None => Response::NullRecord,
        Some(values) => Response::Record { values },
    };
    Ok(response)
}

fn read_transaction_timeout(timeout: Option<i64>) -> TestKitResultT<Option<TransactionTimeout>> {
    match timeout {
        None => Ok(None),
        Some(0) => Ok(Some(TransactionTimeout::none())),
        Some(timeout) => TransactionTimeout::from_millis(timeout)
            .ok_or_else(|| {
                TestKitError::driver_error_client_only(
                    String::from("ConfigureTimeoutError"),
                    String::from("invalid transaction timeout value"),
                    false,
                )
            })
            .map(Some),
    }
}
