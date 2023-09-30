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
use std::ops::Deref;

use serde::Serialize;

use super::backend_id::Generator;
use super::cypher_value::{BrokenValueError, CypherValue};
use super::errors::TestKitError;
use super::requests::TestKitAuth;
use super::session_holder::SummaryWithQuery;
use super::BackendId;

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
    AuthTokenManagerOnAuthExpiredRequest {
        id: BackendId,
        auth_token_manager_id: BackendId,
        auth: TestKitAuth,
    },
    ExpirationBasedAuthTokenManager {
        id: BackendId,
    },
    #[serde(rename_all = "camelCase")]
    ExpirationBasedAuthTokenProviderRequest {
        id: BackendId,
        expiration_based_auth_token_manager_id: BackendId,
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
        id: BackendId,
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
        values: Vec<CypherValue>,
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
    values: Vec<CypherValue>,
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
    query_type: QueryType,
    result_available_after: Option<i64>,
    result_consumed_after: Option<i64>,
    server_info: ServerInfo,
}

impl TryFrom<SummaryWithQuery> for Summary {
    type Error = BrokenValueError;
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
            .map(|(k, v)| (k, v.into()))
            .collect();
        Ok(summary)
    }
}

impl TryFrom<crate::summary::Summary> for Summary {
    type Error = BrokenValueError;
    fn try_from(summary: crate::summary::Summary) -> Result<Self, Self::Error> {
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
            query_type: QueryType::Read,
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

impl From<crate::summary::Counters> for SummaryCounters {
    fn from(counters: crate::summary::Counters) -> Self {
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

impl From<crate::summary::Notification> for Notification {
    fn from(notification: crate::summary::Notification) -> Self {
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

impl From<crate::summary::Position> for Position {
    fn from(position: crate::summary::Position) -> Self {
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
}

impl From<crate::summary::Severity> for Severity {
    fn from(severity: crate::summary::Severity) -> Self {
        match severity {
            crate::summary::Severity::Warning => Self::Warning,
            crate::summary::Severity::Information => Self::Information,
            crate::summary::Severity::Unknown => Self::Unknown,
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
    Unknown,
}

impl From<crate::summary::Category> for Category {
    fn from(severity: crate::summary::Category) -> Self {
        match severity {
            crate::summary::Category::Hint => Self::Hint,
            crate::summary::Category::Unrecognized => Self::Unrecognized,
            crate::summary::Category::Unsupported => Self::Unsupported,
            crate::summary::Category::Performance => Self::Performance,
            crate::summary::Category::Deprecation => Self::Deprecation,
            crate::summary::Category::Generic => Self::Generic,
            crate::summary::Category::Unknown => Self::Unknown,
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Plan {
    args: HashMap<String, CypherValue>,
    operator_type: String,
    identifiers: Vec<String>,
    children: Vec<Plan>,
}

impl TryFrom<crate::summary::Plan> for Plan {
    type Error = BrokenValueError;
    fn try_from(plan: crate::summary::Plan) -> Result<Self, Self::Error> {
        Ok(Self {
            args: plan
                .args
                .into_iter()
                .map(|(k, v)| Ok((k, v.try_into()?)))
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
    args: HashMap<String, CypherValue>,
    operator_type: String,
    identifiers: Vec<String>,
    children: Vec<Profile>,
    db_hits: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_cache_hit_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_cache_hits: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    page_cache_misses: Option<i64>,
    rows: i64,
    time: i64,
}

impl TryFrom<crate::summary::Profile> for Profile {
    type Error = BrokenValueError;
    fn try_from(plan: crate::summary::Profile) -> Result<Self, Self::Error> {
        Ok(Self {
            args: plan
                .args
                .into_iter()
                .map(|(k, v)| Ok((k, v.try_into()?)))
                .collect::<Result<_, _>>()?,
            operator_type: plan.op_type,
            identifiers: plan.identifiers,
            children: plan
                .children
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            db_hits: plan.db_hits,
            page_cache_hit_ratio: Some(plan.page_cache_hit_ratio),
            page_cache_hits: Some(plan.page_cache_hits),
            page_cache_misses: Some(plan.page_cache_misses),
            rows: plan.rows,
            time: plan.time,
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
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServerInfo {
    address: String,
    agent: String,
    protocol_version: String,
}

impl From<crate::summary::ServerInfo> for ServerInfo {
    fn from(server_info: crate::summary::ServerInfo) -> Self {
        Self {
            address: format!("{}", server_info.address),
            agent: server_info.server_agent.deref().clone(),
            protocol_version: format!(
                "{}.{}",
                server_info.protocol_version.0, server_info.protocol_version.1
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn foo() {
        let v = serde_json::to_string(&Response::RecordList(vec![
            RecordListEntry { values: vec![] },
            RecordListEntry {
                values: vec![CypherValue::CypherNull { value: None }],
            },
        ]))
        .unwrap();
        println!("{v}");
    }
}

impl Response {
    pub(super) fn feature_list() -> Self {
        Self::FeatureList {
            features: [
                // "Feature:API:BookmarkManager",
                "Feature:API:ConnectionAcquisitionTimeout",
                // "Feature:API:Driver.ExecuteQuery",
                // "Feature:API:Driver:GetServerInfo",
                // "Feature:API:Driver.IsEncrypted",
                "Feature:API:Driver:NotificationsConfig",
                // "Feature:API:Driver.VerifyConnectivity",
                // "Feature:API:Liveness.Check",
                // "Feature:API:Result.List",
                // "Feature:API:Result.Peek",
                "Feature:API:Result.Single",
                // "Feature:API:Result.SingleOptional",
                // "Feature:API:Session:NotificationsConfig",
                // "Feature:API:SSLConfig",
                // "Feature:API:SSLSchemes",
                "Feature:API:Type.Spatial",
                // "Feature:API:Type.Temporal",
                // "Feature:Auth:Bearer",
                // "Feature:Auth:Custom",
                // "Feature:Auth:Kerberos",
                // "Feature:Bolt:3.0",
                // "Feature:Bolt:4.1",
                // "Feature:Bolt:4.2",
                // "Feature:Bolt:4.3",
                "Feature:Bolt:4.4",
                "Feature:Bolt:5.0",
                // "Feature:Bolt:5.1",
                // "Feature:Bolt:5.2",
                // "Feature:Bolt:Patch:UTC",
                "Feature:Impersonation",
                // "Feature:TLS:1.1",
                // "Feature:TLS:1.2",
                // "Feature:TLS:1.3",

                // "AuthorizationExpiredTreatment",
                "Optimization:ConnectionReuse",
                "Optimization:EagerTransactionBegin",
                "Optimization:ImplicitDefaultArguments",
                "Optimization:MinimalBookmarksSet",
                "Optimization:MinimalResets",
                // "Optimization:AuthPipelining",
                "Optimization:PullPipelining",
                // "Optimization:ResultListFetchAll",

                // "Detail:ClosedDriverIsEncrypted",
                // "Detail:DefaultSecurityConfigValueEquality",
                // "ConfHint:connection.recv_timeout_seconds",

                // "Backend:RTFetch",
                // "Backend:RTForceUpdate",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
        }
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
            } => Response::DriverError {
                id: id.unwrap_or_else(|| id_generator.next_id()),
                error_type,
                msg: Some(msg),
                code,
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
