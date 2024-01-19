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
use std::mem;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::OnceLock;

use lazy_regex::Regex;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::testkit_backend::cypher_value::ConvertableValueReceive;
use neo4j::driver::EagerResult;
use neo4j::summary::SummaryQueryType;
use neo4j::value::time::Tz;
use neo4j::ValueSend;

use super::backend_id::Generator;
use super::cypher_value::{CypherValue, CypherValues};
use super::errors::TestKitError;
use super::requests::TestKitAuth;
use super::session_holder::SummaryWithQuery;
use super::BackendId;

// https://github.com/rust-lang/rust/issues/85077
const FEATURE_LIST: [&str; 42] = [
    // === FUNCTIONAL FEATURES ===
    "Feature:API:BookmarkManager",
    "Feature:API:ConnectionAcquisitionTimeout",
    "Feature:API:Driver.ExecuteQuery",
    "Feature:API:Driver:GetServerInfo",
    "Feature:API:Driver.IsEncrypted",
    // Even tough the driver does not support notification config,
    // TestKit uses this flag to change assertions on the notification objects
    "Feature:API:Driver:NotificationsConfig",
    "Feature:API:Driver.VerifyAuthentication",
    "Feature:API:Driver.VerifyConnectivity",
    "Feature:API:Driver.SupportsSessionAuth",
    "Feature:API:Liveness.Check",
    // "Feature:API:Result.List",
    // "Feature:API:Result.Peek",
    "Feature:API:Result.Single",
    // "Feature:API:Result.SingleOptional",
    "Feature:API:RetryableExceptions",
    "Feature:API:Session:AuthConfig",
    "Feature:API:Session:NotificationsConfig",
    "Feature:API:SSLConfig",
    "Feature:API:SSLSchemes",
    "Feature:API:Type.Spatial",
    "Feature:API:Type.Temporal",
    "Feature:Auth:Bearer",
    "Feature:Auth:Custom",
    "Feature:Auth:Kerberos",
    "Feature:Auth:Managed",
    // "Feature:Bolt:3.0",
    // "Feature:Bolt:4.1",
    // "Feature:Bolt:4.2",
    // "Feature:Bolt:4.3",
    "Feature:Bolt:4.4",
    "Feature:Bolt:5.0",
    "Feature:Bolt:5.1",
    "Feature:Bolt:5.2",
    // "Feature:Bolt:5.3",
    // "Feature:Bolt:5.4",
    "Feature:Bolt:Patch:UTC",
    "Feature:Impersonation",
    // "Feature:TLS:1.1",  // rustls says no! For a good reason.
    "Feature:TLS:1.2",
    "Feature:TLS:1.3",
    //
    // === OPTIMIZATIONS ===
    "AuthorizationExpiredTreatment",
    "Optimization:AuthPipelining",
    "Optimization:ConnectionReuse",
    "Optimization:EagerTransactionBegin",
    "Optimization:ExecuteQueryPipelining",
    "Optimization:ImplicitDefaultArguments",
    "Optimization:MinimalBookmarksSet",
    "Optimization:MinimalResets",
    "Optimization:MinimalVerifyAuthentication",
    "Optimization:PullPipelining",
    // "Optimization:ResultListFetchAll",
    //
    // === IMPLEMENTATION DETAILS ===
    // "Detail:ClosedDriverIsEncrypted",
    // "Detail:DefaultSecurityConfigValueEquality",
    //
    // === CONFIGURATION HINTS (BOLT 4.3+) ===
    "ConfHint:connection.recv_timeout_seconds",
    //
    // === BACKEND FEATURES FOR TESTING ===
    "Backend:MockTime",
    // "Backend:RTFetch",
    // "Backend:RTForceUpdate",
];

static PLAIN_SKIPPED_TESTS: OnceLock<HashMap<&'static str, &'static str>> = OnceLock::new();
static REGEX_SKIPPED_TESTS: OnceLock<Vec<(&'static Regex, &'static str)>> = OnceLock::new();

fn get_plain_skipped_tests() -> &'static HashMap<&'static str, &'static str> {
    PLAIN_SKIPPED_TESTS.get_or_init(|| {
        HashMap::from([
            // ("path.to.skipped_test", "reason"),
        ])
    })
}

fn get_regex_skipped_tests() -> &'static [(&'static Regex, &'static str)] {
    REGEX_SKIPPED_TESTS.get_or_init(|| {
        vec![
            // use lazy_regex::regex;
            // (regex!(r"^test_.*$"), "reason"),
        ]
    })
}

#[allow(dead_code)] // reflects TestKit protocol
#[derive(Serialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(super) enum Response {
    FeatureList {
        features: Vec<String>,
    },
    RunTest,
    RunSubTests,
    SkipTest {
        reason: String,
    },
    Driver {
        id: BackendId,
    },
    AuthTokenManager {
        id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    AuthTokenManagerGetAuthRequest {
        id: BackendId,
        auth_token_manager_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    AuthTokenManagerHandleSecurityExceptionRequest {
        id: BackendId,
        auth_token_manager_id: BackendId,
        auth: TestKitAuth,
        error_code: String,
    },
    BasicAuthTokenManager {
        id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    BasicAuthTokenProviderRequest {
        id: BackendId,
        basic_auth_token_manager_id: BackendId,
    },
    BearerAuthTokenManager {
        id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    BearerAuthTokenProviderRequest {
        id: BackendId,
        bearer_auth_token_manager_id: BackendId,
    },
    ResolverResolutionRequired {
        id: BackendId,
        address: String,
    },
    BookmarkManager {
        id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    BookmarksSupplierRequest {
        id: BackendId,
        bookmark_manager_id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    BookmarksConsumerRequest {
        id: BackendId,
        bookmark_manager_id: BackendId,
        bookmarks: Vec<String>,
    },
    DomainNameResolutionRequired {
        id: BackendId,
        name: String,
    },
    MultiDBSupport {
        id: BackendId,
        available: bool,
    },
    DriverIsAuthenticated {
        id: BackendId,
        authenticated: bool,
    },
    SessionAuthSupport {
        id: BackendId,
        available: bool,
    },
    DriverIsEncrypted {
        encrypted: bool,
    },
    Session {
        id: BackendId,
    },
    Transaction {
        id: BackendId,
    },
    Result {
        id: BackendId,
        keys: Vec<String>,
    },
    Record {
        values: CypherValues,
    },
    Field {
        value: CypherValue,
    },
    NullRecord,
    RecordList(Vec<RecordListEntry>),
    RecordOptional {
        record: Option<RecordListEntry>,
        warnings: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    Summary(Summary),
    ServerInfo(ServerInfo),
    Bookmarks {
        bookmarks: Vec<String>,
    },
    RetryableTry {
        id: BackendId,
    },
    RetryableDone,
    RoutingTable {
        database: String,
        ttl: i64,
        routers: Vec<String>,
        readers: Vec<String>,
        writers: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    ConnectionPoolMetrics {
        in_use: usize,
        idle: usize,
    },
    EagerResult {
        keys: Vec<String>,
        records: Vec<RecordListEntry>,
        summary: Summary,
    },
    FakeTimeAck,
    #[serde(rename_all = "camelCase")]
    DriverError {
        id: BackendId,
        error_type: String,
        msg: Option<String>,
        code: Option<String>,
        retryable: bool,
    },
    FrontendError {
        msg: String,
    },
    BackendError {
        msg: String,
    },
}

#[derive(Serialize, Debug)]
pub(super) struct RecordListEntry {
    values: CypherValues,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Summary {
    counters: SummaryCounters,
    database: Option<String>,
    notifications: Option<Vec<Notification>>,
    plan: Option<Plan>,
    profile: Option<Profile>,
    query: SummaryQuery,
    query_type: Option<QueryType>,
    result_available_after: Option<i64>,
    result_consumed_after: Option<i64>,
    server_info: ServerInfo,
}

impl TryFrom<SummaryWithQuery> for Summary {
    type Error = TestKitError;

    fn try_from(summary: SummaryWithQuery) -> Result<Self, Self::Error> {
        let SummaryWithQuery {
            summary,
            query,
            parameters,
        } = summary;
        let mut summary: Self = (*summary).clone().try_into()?;
        summary.query.text = (*query).clone();
        summary.query.parameters = (*parameters)
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| v.try_into().map(|v| (k, v)))
            .collect::<Result<_, _>>()?;
        Ok(summary)
    }
}

impl TryFrom<neo4j::summary::Summary> for Summary {
    type Error = TestKitError;

    fn try_from(summary: neo4j::summary::Summary) -> Result<Self, Self::Error> {
        Ok(Self {
            counters: summary.counters.into(),
            database: summary.database,
            notifications: summary
                .notifications
                .map(|notifications| notifications.into_iter().map(Into::into).collect()),
            plan: summary.plan.map(TryInto::try_into).transpose()?,
            profile: summary.profile.map(TryInto::try_into).transpose()?,
            query: SummaryQuery {
                // Is filled later from request handler because the rust driver
                // avoids copying around the data. It's left to the client code
                // (TestKit backend in this case) to keep track of the sent
                // query and parameters.
                text: Default::default(),
                parameters: Default::default(),
            },
            query_type: summary.query_type.map(Into::into),
            result_available_after: summary.result_available_after.map(|d| {
                d.as_millis()
                    .try_into()
                    .expect("Server can only send i64::MAX milliseconds")
            }),
            result_consumed_after: summary.result_consumed_after.map(|d| {
                d.as_millis()
                    .try_into()
                    .expect("Server can only send i64::MAX milliseconds")
            }),
            server_info: summary.server_info.into(),
        })
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct SummaryCounters {
    constraints_added: i64,
    constraints_removed: i64,
    contains_system_updates: bool,
    contains_updates: bool,
    indexes_added: i64,
    indexes_removed: i64,
    labels_added: i64,
    labels_removed: i64,
    nodes_created: i64,
    nodes_deleted: i64,
    properties_set: i64,
    relationships_created: i64,
    relationships_deleted: i64,
    system_updates: i64,
}

impl From<neo4j::summary::Counters> for SummaryCounters {
    fn from(counters: neo4j::summary::Counters) -> Self {
        Self {
            constraints_added: counters.constraints_added,
            constraints_removed: counters.constraints_removed,
            contains_system_updates: counters.contains_system_updates,
            contains_updates: counters.contains_updates,
            indexes_added: counters.indexes_added,
            indexes_removed: counters.indexes_removed,
            labels_added: counters.labels_added,
            labels_removed: counters.labels_removed,
            nodes_created: counters.nodes_created,
            nodes_deleted: counters.nodes_deleted,
            properties_set: counters.properties_set,
            relationships_created: counters.relationships_created,
            relationships_deleted: counters.relationships_deleted,
            system_updates: counters.system_updates,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Notification {
    description: String,
    code: String,
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    position: Option<Position>,
    // severity for backwards compatibility
    severity: String,
    severity_level: Severity,
    raw_severity_level: String,
    category: Category,
    raw_category: String,
}

impl From<neo4j::summary::Notification> for Notification {
    fn from(notification: neo4j::summary::Notification) -> Self {
        Self {
            description: notification.description,
            code: notification.code,
            title: notification.title,
            position: notification.position.map(Into::into),
            severity: notification.raw_severity.clone(),
            severity_level: notification.severity.into(),
            raw_severity_level: notification.raw_severity,
            category: notification.category.into(),
            raw_category: notification.raw_category,
        }
    }
}

#[derive(Serialize, Debug)]
pub(super) struct Position {
    column: i64,
    offset: i64,
    line: i64,
}

impl From<neo4j::summary::Position> for Position {
    fn from(position: neo4j::summary::Position) -> Self {
        Self {
            column: position.column,
            offset: position.offset,
            line: position.line,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum Severity {
    Warning,
    Information,
    Unknown,
    Unhandled,
}

impl From<neo4j::summary::Severity> for Severity {
    fn from(severity: neo4j::summary::Severity) -> Self {
        match severity {
            neo4j::summary::Severity::Warning => Self::Warning,
            neo4j::summary::Severity::Information => Self::Information,
            neo4j::summary::Severity::Unknown => Self::Unknown,
            _ => Self::Unhandled,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
pub(super) enum Category {
    Hint,
    Unrecognized,
    Unsupported,
    Performance,
    Deprecation,
    Generic,
    Security,
    Topology,
    Unknown,
    Unhandled,
}

impl From<neo4j::summary::Category> for Category {
    fn from(severity: neo4j::summary::Category) -> Self {
        match severity {
            neo4j::summary::Category::Hint => Self::Hint,
            neo4j::summary::Category::Unrecognized => Self::Unrecognized,
            neo4j::summary::Category::Unsupported => Self::Unsupported,
            neo4j::summary::Category::Performance => Self::Performance,
            neo4j::summary::Category::Deprecation => Self::Deprecation,
            neo4j::summary::Category::Generic => Self::Generic,
            neo4j::summary::Category::Security => Self::Security,
            neo4j::summary::Category::Topology => Self::Topology,
            neo4j::summary::Category::Unknown => Self::Unknown,
            _ => Self::Unhandled,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Plan {
    args: HashMap<String, JsonValue>,
    operator_type: String,
    identifiers: Vec<String>,
    children: Vec<Plan>,
}

impl TryFrom<neo4j::summary::Plan> for Plan {
    type Error = TestKitError;

    fn try_from(plan: neo4j::summary::Plan) -> Result<Self, Self::Error> {
        Ok(Self {
            args: plan
                .args
                .into_iter()
                .map(|(k, v)| {
                    Ok::<_, Self::Error>((
                        k,
                        ConvertableValueReceive(v)
                            .try_into()
                            .map_err(TestKitError::backend_err)?,
                    ))
                })
                .collect::<Result<_, _>>()?,
            operator_type: plan.op_type,
            identifiers: plan.identifiers,
            children: plan
                .children
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Profile {
    args: HashMap<String, JsonValue>,
    operator_type: String,
    identifiers: Vec<String>,
    children: Vec<Profile>,
    db_hits: i64,
    rows: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_cache_hit_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_cache_hits: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_cache_misses: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time: Option<i64>,
}

impl TryFrom<neo4j::summary::Profile> for Profile {
    type Error = TestKitError;

    fn try_from(profile: neo4j::summary::Profile) -> Result<Self, Self::Error> {
        Ok(Self {
            args: profile
                .args
                .into_iter()
                .map(|(k, v)| {
                    Ok::<_, Self::Error>((
                        k,
                        ConvertableValueReceive(v)
                            .try_into()
                            .map_err(TestKitError::backend_err)?,
                    ))
                })
                .collect::<Result<_, _>>()?,
            operator_type: profile.op_type,
            identifiers: profile.identifiers,
            children: profile
                .children
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            db_hits: profile.db_hits,
            rows: profile.rows,
            page_cache_hit_ratio: match profile.has_page_cache_stats {
                true => Some(profile.page_cache_hit_ratio),
                false => None,
            },
            page_cache_hits: match profile.has_page_cache_stats {
                true => Some(profile.page_cache_hits),
                false => None,
            },
            page_cache_misses: match profile.has_page_cache_stats {
                true => Some(profile.page_cache_misses),
                false => None,
            },
            time: match profile.has_page_cache_stats {
                true => Some(profile.time),
                false => None,
            },
        })
    }
}

#[derive(Serialize, Debug)]
pub(super) struct SummaryQuery {
    text: String,
    parameters: HashMap<String, CypherValue>,
}

#[derive(Serialize, Debug)]
pub(super) enum QueryType {
    #[serde(rename = "r")]
    Read,
    #[serde(rename = "w")]
    Write,
    #[serde(rename = "rw")]
    ReadWrite,
    #[serde(rename = "s")]
    Schema,
    #[serde(rename = "???")]
    Unhandled,
}

impl From<SummaryQueryType> for QueryType {
    fn from(value: SummaryQueryType) -> Self {
        match value {
            SummaryQueryType::Read => Self::Read,
            SummaryQueryType::Write => Self::Write,
            SummaryQueryType::ReadWrite => Self::ReadWrite,
            SummaryQueryType::Schema => Self::Schema,
            _ => Self::Unhandled,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServerInfo {
    address: String,
    agent: String,
    protocol_version: String,
}

impl From<neo4j::summary::ServerInfo> for ServerInfo {
    fn from(server_info: neo4j::summary::ServerInfo) -> Self {
        Self {
            address: server_info.address.to_string(),
            agent: server_info.server_agent.deref().clone(),
            protocol_version: format!(
                "{}.{}",
                server_info.protocol_version.0, server_info.protocol_version.1
            ),
        }
    }
}

impl TryFrom<EagerResult> for Response {
    type Error = TestKitError;

    fn try_from(value: EagerResult) -> Result<Self, Self::Error> {
        let EagerResult {
            keys,
            records,
            summary,
        } = value;
        let keys = keys.into_iter().map(|k| (*k).clone()).collect();
        let records = records
            .into_iter()
            .map(|r| {
                Ok(RecordListEntry {
                    values: r
                        .into_values()
                        .map(|e| Ok(e.try_into()?))
                        .collect::<Result<_, TestKitError>>()?,
                })
            })
            .collect::<Result<_, TestKitError>>()?;
        // let records: RecordListEntry = result.records.map(|r| {let values = r.entries.map(|e| e.1.try_into()).collect::<Result<Vec<_>, TestKitError>>()?;Ok(RecordListEntry { values }}))
        let summary = summary.try_into()?;
        Ok(Self::EagerResult {
            keys,
            records,
            summary,
        })
    }
}

impl Response {
    pub(super) fn feature_list() -> Self {
        Self::FeatureList {
            features: FEATURE_LIST.into_iter().map(String::from).collect(),
        }
    }

    pub(super) fn run_test(test_name: String) -> Self {
        if let Some(reason) = get_plain_skipped_tests().get(test_name.as_str()) {
            return Self::SkipTest {
                reason: reason.to_string(),
            };
        }
        for (regex, reason) in get_regex_skipped_tests() {
            if regex.is_match(test_name.as_str()) {
                return Self::SkipTest {
                    reason: reason.to_string(),
                };
            }
        }
        match test_name.as_str() {
            "neo4j.datatypes.test_temporal_types.TestDataTypes.test_date_time_cypher_created_tz_id" 
            | "neo4j.datatypes.test_temporal_types.TestDataTypes.test_should_echo_all_timezone_ids" =>
                return Self::RunSubTests,
            _ => {}
        }
        Self::RunTest
    }

    pub(super) fn run_sub_test(
        test_name: String,
        arguments: HashMap<String, JsonValue>,
    ) -> Result<Self, TestKitError> {
        match test_name.as_str() {
            "neo4j.datatypes.test_temporal_types.TestDataTypes.test_date_time_cypher_created_tz_id" =>
                Self::run_sub_test_test_date_time_cypher_created_tz_id(arguments),
            "neo4j.datatypes.test_temporal_types.TestDataTypes.test_should_echo_all_timezone_ids" =>
                Self::run_sub_test_test_should_echo_all_timezone_ids(arguments),
            _ => Err(TestKitError::backend_err(
                format!("Backend didn't request to check sub tests for {test_name}")
            )),
        }
    }

    fn run_sub_test_test_date_time_cypher_created_tz_id(
        arguments: HashMap<String, JsonValue>,
    ) -> Result<Self, TestKitError> {
        let tz_id = arguments
            .get("tz_id")
            .ok_or_else(|| TestKitError::backend_err("expected key `tz_id` in arguments"))?
            .as_str()
            .ok_or_else(|| TestKitError::backend_err("expected `tz_id` to be a string"))?;
        Ok(Tz::from_str(tz_id)
            .map(|_| Self::RunTest)
            .unwrap_or_else(|e| Self::SkipTest {
                reason: format!("cannot load timezone {tz_id}: {e}"),
            }))
    }

    fn run_sub_test_test_should_echo_all_timezone_ids(
        mut arguments: HashMap<String, JsonValue>,
    ) -> Result<Self, TestKitError> {
        fn get_opt_i64_component(data: &JsonValue, key: &str) -> Result<Option<i64>, TestKitError> {
            match data.get(key) {
                None => Ok(None),
                Some(v) => match v.as_i64() {
                    None => Err(TestKitError::backend_err(format!(
                        "CypherDateTime value missing `{key}`"
                    ))),
                    Some(v) => Ok(Some(v)),
                },
            }
        }

        fn get_i64_component(data: &JsonValue, key: &str) -> Result<i64, TestKitError> {
            get_opt_i64_component(data, key).and_then(|v| {
                v.ok_or_else(|| {
                    TestKitError::backend_err(format!(
                        "CypherDateTime value `{key}` is not an integer"
                    ))
                })
            })
        }

        fn get_string_opt_component(
            data: &mut JsonValue,
            key: &str,
        ) -> Result<Option<String>, TestKitError> {
            match data.get_mut(key) {
                None => Ok(None),
                Some(v) => match v {
                    JsonValue::String(s) => Ok(Some(mem::take(s))),
                    _ => Err(TestKitError::backend_err(format!(
                        "CypherDateTime value `{key}` is not a string"
                    ))),
                },
            }
        }

        let dt = arguments
            .get_mut("dt")
            .ok_or_else(|| TestKitError::backend_err("expected key `dt` in arguments"))?
            .as_object_mut()
            .ok_or_else(|| TestKitError::backend_err("expected key `dt` to be an object"))?;
        match dt.get("name").and_then(|v| v.as_str()) {
            Some("CypherDateTime") => {}
            _ => {
                return Err(TestKitError::backend_err(
                    "expected key `dt` to be a CypherDateTime",
                ))
            }
        };
        let data = dt
            .get_mut("data")
            .ok_or_else(|| TestKitError::backend_err("CypherDateTime value missing `data`"))?;
        let year = get_i64_component(data, "year")?;
        let month = get_i64_component(data, "month")?;
        let day = get_i64_component(data, "day")?;
        let hour = get_i64_component(data, "hour")?;
        let minute = get_i64_component(data, "minute")?;
        let second = get_i64_component(data, "second")?;
        let nanosecond = get_i64_component(data, "nanosecond")?;
        let utc_offset_s = get_opt_i64_component(data, "utc_offset_s")?;
        let timezone_id = get_string_opt_component(data, "timezone_id")?;

        let value = CypherValue::CypherDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            utc_offset_s,
            timezone_id,
        };
        Ok(ValueSend::try_from(value)
            .map(|_| Self::RunTest)
            .unwrap_or_else(|e| Self::SkipTest {
                reason: format!("cannot load CypherDateTime: {e}"),
            }))
    }

    pub(super) fn try_from_testkit_error(
        e: TestKitError,
        id_generator: &Generator,
    ) -> Result<Response, TestKitError> {
        Ok(match e {
            TestKitError::DriverError {
                error_type,
                msg,
                code,
                id,
                retryable,
            } => Response::DriverError {
                id: id.unwrap_or_else(|| id_generator.next_id()),
                error_type,
                msg: Some(msg),
                code,
                retryable,
            },
            TestKitError::FrontendError { msg } => Response::FrontendError { msg },
            TestKitError::BackendError { msg } => Response::BackendError { msg },
            e @ TestKitError::FatalError { .. } => {
                return Err(TestKitError::backend_err(format!(
                    "cannot serialize FatalError (bug in backend): {e}"
                )))
            }
        })
    }
}
