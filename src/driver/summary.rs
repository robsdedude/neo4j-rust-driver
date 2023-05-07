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

use super::io::bolt::BoltMeta;
use super::io::PooledBolt;
use crate::{Address, Neo4jError, Result, ValueReceive};

/// Root struct containing query meta data.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Summary {
    pub result_available_after: i64,
    pub result_consumed_after: i64,
    pub counters: Counters,
    pub notifications: Option<Vec<Notification>>,
    pub profile: Option<Profile>,
    pub plan: Option<Plan>,
    pub query_type: SummaryQueryType,
    pub database: Option<String>,
    pub server_info: ServerInfo,
}

impl Summary {
    pub(crate) fn new(connection: &PooledBolt) -> Self {
        Self {
            result_available_after: Default::default(),
            result_consumed_after: Default::default(),
            counters: Default::default(),
            notifications: Default::default(),
            profile: Default::default(),
            plan: Default::default(),
            query_type: Default::default(),
            database: Default::default(),
            server_info: ServerInfo {
                address: connection.address(),
                server_agent: connection.server_agent(),
                protocol_version: connection.protocol_version(),
            },
        }
    }

    pub(crate) fn load_run_meta(&mut self, meta: &mut BoltMeta) -> Result<()> {
        if let Some(t_first) = meta.remove("t_first") {
            let ValueReceive::Integer(t_first) = t_first else {
                return Err(Neo4jError::protocol_error(format!("t_first in summary was not integer but {:?}", t_first)))
            };
            self.result_available_after = t_first;
        }
        Ok(())
    }
    pub(crate) fn load_pull_meta(&mut self, meta: &mut BoltMeta) -> Result<()> {
        if let Some(t_last) = meta.remove("t_last") {
            let ValueReceive::Integer(t_last) = t_last else {
                return Err(Neo4jError::protocol_error(format!("t_last in summary was not integer but {:?}", t_last)))
            };
            self.result_consumed_after = t_last;
        }
        self.counters = Counters::load_meta(meta)?;
        self.notifications = Notification::load_meta(meta)?;
        self.profile = Profile::load_meta(meta)?;
        self.plan = Plan::load_meta(meta)?;
        if let Some(query_type) = meta.remove("type") {
            let ValueReceive::String(query_type) = query_type else {
                return Err(Neo4jError::protocol_error(format!("type in summary was not string but {:?}", query_type)))
            };
            self.query_type = match query_type.as_str() {
                "r" => SummaryQueryType::Read,
                "w" => SummaryQueryType::Write,
                "rw" => SummaryQueryType::ReadWrite,
                "s" => SummaryQueryType::Schema,
                _ => {
                    return Err(Neo4jError::protocol_error(format!(
                        "type in summary was an unknown string {:?}",
                        query_type
                    )))
                }
            };
        } else {
            return Err(Neo4jError::protocol_error("type in summary missing"));
        }
        if let Some(db) = meta.remove("db") {
            let ValueReceive::String(db) = db else {
                return Err(Neo4jError::protocol_error(format!("db in summary was not string but {:?}", db)))
            };
            self.database = Some(db);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Counters {
    pub nodes_created: i64,
    pub nodes_deleted: i64,
    pub relationships_created: i64,
    pub relationships_deleted: i64,
    pub properties_set: i64,
    pub labels_added: i64,
    pub labels_removed: i64,
    pub indexes_added: i64,
    pub indexes_removed: i64,
    pub constraints_added: i64,
    pub constraints_removed: i64,
    pub system_updates: i64,
    pub contains_updates: bool,
    pub contains_system_updates: bool,
}

impl Counters {
    fn load_meta(meta: &mut BoltMeta) -> Result<Self> {
        let Some(meta) = meta.remove("stats") else {
            return Ok(Counters {
                nodes_created: 0,
                nodes_deleted: 0,
                relationships_created: 0,
                relationships_deleted: 0,
                properties_set: 0,
                labels_added: 0,
                labels_removed: 0,
                indexes_added: 0,
                indexes_removed: 0,
                constraints_added: 0,
                constraints_removed: 0,
                system_updates: 0,
                contains_updates: false,
                contains_system_updates: false,
            })
        };
        let mut meta = try_into_map(meta, "stats")?;
        let nodes_created = meta
            .remove("nodes-created")
            .map(|c| try_into_int(c, "nodes-created in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let nodes_deleted = meta
            .remove("nodes-deleted")
            .map(|c| try_into_int(c, "nodes-deleted in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let relationships_created = meta
            .remove("relationships-created")
            .map(|c| try_into_int(c, "relationships-created in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let relationships_deleted = meta
            .remove("relationships-deleted")
            .map(|c| try_into_int(c, "relationships-deleted in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let properties_set = meta
            .remove("properties-set")
            .map(|c| try_into_int(c, "properties-set in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let labels_added = meta
            .remove("labels-added")
            .map(|c| try_into_int(c, "labels-added in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let labels_removed = meta
            .remove("labels-removed")
            .map(|c| try_into_int(c, "labels-removed in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let indexes_added = meta
            .remove("indexes-added")
            .map(|c| try_into_int(c, "indexes-added in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let indexes_removed = meta
            .remove("indexes-removed")
            .map(|c| try_into_int(c, "indexes-removed in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let constraints_added = meta
            .remove("constraints-added")
            .map(|c| try_into_int(c, "constraints-added in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let constraints_removed = meta
            .remove("constraints-removed")
            .map(|c| try_into_int(c, "constraints-removed in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let system_updates = meta
            .remove("system-updates")
            .map(|c| try_into_int(c, "system-updates in stats"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let contains_updates = meta
            .remove("contains-updates")
            .map(|c| try_into_bool(c, "contains-updates in stats"))
            .unwrap_or_else(|| {
                Ok(nodes_created > 0
                    || nodes_deleted > 0
                    || relationships_created > 0
                    || relationships_deleted > 0
                    || properties_set > 0
                    || labels_added > 0
                    || labels_removed > 0
                    || indexes_added > 0
                    || indexes_removed > 0
                    || constraints_added > 0
                    || constraints_removed > 0)
            })?;
        let contains_system_updates = meta
            .remove("contains-system-updates")
            .map(|c| try_into_bool(c, "contains-system-updates in stats"))
            .unwrap_or_else(|| Ok(system_updates > 0))?;
        Ok(Self {
            nodes_created,
            nodes_deleted,
            relationships_created,
            relationships_deleted,
            properties_set,
            labels_added,
            labels_removed,
            indexes_added,
            indexes_removed,
            constraints_added,
            constraints_removed,
            system_updates,
            contains_updates,
            contains_system_updates,
        })
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum SummaryQueryType {
    #[default]
    Read,
    Write,
    ReadWrite,
    Schema,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Notification {
    pub description: String,
    pub code: String,
    pub title: String,
    pub position: Position,
    pub severity: Severity,
    pub raw_severity: String,
    pub category: Category,
    pub raw_category: String,
}

impl Notification {
    fn load_meta(meta: &mut BoltMeta) -> Result<Option<Vec<Self>>> {
        let Some(notifications) = meta.remove("notifications") else {
            return Ok(None)
        };
        Ok(Some(
            try_into_list(notifications, "notifications")?
                .into_iter()
                .map(|n| Self::load_single(&mut try_into_map(n, "notifications entry")?))
                .collect::<Result<Vec<Self>>>()?,
        ))
    }

    fn load_single(meta: &mut BoltMeta) -> Result<Self> {
        let description = meta
            .remove("description")
            .map(|c| try_into_string(c, "description in notification"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let code = meta
            .remove("code")
            .map(|c| try_into_string(c, "code in notification"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let title = meta
            .remove("title")
            .map(|c| try_into_string(c, "title in notification"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let position = meta
            .remove("position")
            .map(|c| {
                let mut meta = try_into_map(c, "position in notification")?;
                Position::load_meta(&mut meta)
            })
            .unwrap_or_else(|| {
                Ok(Position {
                    column: 0,
                    offset: 0,
                    line: 0,
                })
            })?;
        let raw_severity = meta
            .remove("severity")
            .map(|c| try_into_string(c, "severity in notification"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let severity = Severity::from_str(&raw_severity);
        let raw_category = meta
            .remove("category")
            .map(|c| try_into_string(c, "category in notification"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let category = Category::from_str(&raw_category);
        Ok(Self {
            description,
            code,
            title,
            position,
            severity,
            raw_severity,
            category,
            raw_category,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Position {
    /// The column number referred to by the position; column numbers start at 1.
    pub column: i64,
    /// The character offset referred to by this position; offset numbers start at 0.
    pub offset: i64,
    /// The line number referred to by the position; line numbers start at 1.
    pub line: i64,
}

impl Position {
    fn load_meta(meta: &mut BoltMeta) -> Result<Self> {
        let column = meta
            .remove("column")
            .map(|c| try_into_int(c, "column in notification position"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let offset = meta
            .remove("offset")
            .map(|c| try_into_int(c, "offset in notification position"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let line = meta
            .remove("line")
            .map(|c| try_into_int(c, "line in notification position"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        Ok(Self {
            column,
            offset,
            line,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Severity {
    Warning,
    Information,
    Unknown,
}

impl Severity {
    fn from_str(s: &str) -> Self {
        match s {
            "WARNING" => Self::Warning,
            "INFORMATION" => Self::Information,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum Category {
    Hint,
    Unrecognized,
    Unsupported,
    Performance,
    Deprecation,
    Generic,
    Unknown,
}

impl Category {
    fn from_str(s: &str) -> Self {
        match s {
            "HINT" => Self::Hint,
            "UNRECOGNIZED" => Self::Unrecognized,
            "UNSUPPORTED" => Self::Unsupported,
            "PERFORMANCE" => Self::Performance,
            "DEPRECATION" => Self::Deprecation,
            "GENERIC" => Self::Generic,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug)]
struct PlanProfileCommon {
    pub args: HashMap<String, ValueReceive>,
    pub op_type: String,
    pub identifiers: Vec<String>,
}

impl PlanProfileCommon {
    fn load_single(meta: &mut BoltMeta) -> Result<Self> {
        let args = meta
            .remove("args")
            .map(|v| try_into_map(v, "args in plan/profile"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let op_type = meta
            .remove("operatorType")
            .map(|v| try_into_string(v, "operatorType in plan/profile"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let identifiers = meta
            .remove("operatorType")
            .map(|v| {
                try_into_list(v, "identifiers in plan/profile")?
                    .into_iter()
                    .map(|e| try_into_string(e, "element in identifiers in plan/profile"))
                    .collect()
            })
            .unwrap_or_else(|| Ok(Default::default()))?;
        Ok(Self {
            args,
            op_type,
            identifiers,
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Plan {
    pub args: HashMap<String, ValueReceive>,
    pub op_type: String,
    pub identifiers: Vec<String>,
    pub children: Vec<Plan>,
}

impl Plan {
    fn load_meta(meta: &mut BoltMeta) -> Result<Option<Self>> {
        meta.remove("plan")
            .map(|v| {
                let mut v = try_into_map(v, "plan in summary")?;
                Ok(Some(Self::load_single(&mut v)?))
            })
            .unwrap_or(Ok(None))
    }

    fn load_single(meta: &mut BoltMeta) -> Result<Self> {
        let PlanProfileCommon {
            args,
            op_type,
            identifiers,
        } = PlanProfileCommon::load_single(meta)?;
        let children = meta
            .remove("children")
            .map(|v| {
                try_into_list(v, "children in plan")?
                    .into_iter()
                    .map(|v| {
                        Self::load_single(&mut try_into_map(v, "element in children in plan")?)
                    })
                    .collect()
            })
            .unwrap_or_else(|| Ok(Default::default()))?;
        Ok(Self {
            args,
            op_type,
            identifiers,
            children,
        })
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Profile {
    pub args: HashMap<String, ValueReceive>,
    pub op_type: String,
    pub identifiers: Vec<String>,
    pub children: Vec<Profile>,
    pub db_hits: i64,
    pub has_page_cache_stats: bool,
    pub page_cache_hit_ratio: f64,
    pub page_cache_hits: i64,
    pub page_cache_misses: i64,
    pub rows: i64,
    pub time: i64,
}

impl Profile {
    fn load_meta(meta: &mut BoltMeta) -> Result<Option<Self>> {
        meta.remove("profile")
            .map(|v| {
                let mut v = try_into_map(v, "profile in summary")?;
                Ok(Some(Self::load_single(&mut v)?))
            })
            .unwrap_or(Ok(None))
    }

    fn load_single(meta: &mut BoltMeta) -> Result<Self> {
        let PlanProfileCommon {
            args,
            op_type,
            identifiers,
        } = PlanProfileCommon::load_single(meta)?;
        let children = meta
            .remove("children")
            .map(|v| {
                try_into_list(v, "children in profile")?
                    .into_iter()
                    .map(|v| {
                        Self::load_single(&mut try_into_map(v, "element in children in profile")?)
                    })
                    .collect()
            })
            .unwrap_or_else(|| Ok(Default::default()))?;
        let db_hits = meta
            .remove("dbHits")
            .map(|v| try_into_int(v, "dbHits in profile"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let page_cache_hit_ratio = meta
            .remove("pageCacheHitRatio")
            .map(|v| try_into_float(v, "pageCacheHitRatio in profile").map(Some))
            .unwrap_or(Ok(None))?;
        let page_cache_hits = meta
            .remove("pageCacheHits")
            .map(|v| try_into_int(v, "pageCacheHits in profile").map(Some))
            .unwrap_or(Ok(None))?;
        let page_cache_misses = meta
            .remove("pageCacheMisses")
            .map(|v| try_into_int(v, "pageCacheMisses in profile").map(Some))
            .unwrap_or(Ok(None))?;
        let has_page_cache_stats = page_cache_hit_ratio.is_some()
            || page_cache_hits.is_some()
            || page_cache_misses.is_some();
        let page_cache_hit_ratio = page_cache_hit_ratio.unwrap_or_default();
        let page_cache_hits = page_cache_hits.unwrap_or_default();
        let page_cache_misses = page_cache_misses.unwrap_or_default();
        let rows = meta
            .remove("rows")
            .map(|v| try_into_int(v, "rows in profile"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        let time = meta
            .remove("time")
            .map(|v| try_into_int(v, "time in profile"))
            .unwrap_or_else(|| Ok(Default::default()))?;
        Ok(Self {
            args,
            op_type,
            identifiers,
            children,
            db_hits,
            has_page_cache_stats,
            page_cache_hit_ratio,
            page_cache_hits,
            page_cache_misses,
            rows,
            time,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub address: Arc<Address>,
    pub server_agent: Arc<String>,
    pub protocol_version: (u8, u8),
}

fn try_into_bool(v: ValueReceive, context: &str) -> Result<bool> {
    v.try_into_bool()
        .map_err(|v| Neo4jError::protocol_error(format!("{} was not bool but {:?}", context, v)))
}

fn try_into_int(v: ValueReceive, context: &str) -> Result<i64> {
    v.try_into_int()
        .map_err(|v| Neo4jError::protocol_error(format!("{} was not int but {:?}", context, v)))
}

fn try_into_float(v: ValueReceive, context: &str) -> Result<f64> {
    v.try_into_float()
        .map_err(|v| Neo4jError::protocol_error(format!("{} was not float but {:?}", context, v)))
}

fn try_into_string(v: ValueReceive, context: &str) -> Result<String> {
    v.try_into_string()
        .map_err(|v| Neo4jError::protocol_error(format!("{} was not string but {:?}", context, v)))
}

fn try_into_list(v: ValueReceive, context: &str) -> Result<Vec<ValueReceive>> {
    v.try_into_list()
        .map_err(|v| Neo4jError::protocol_error(format!("{} was not list but {:?}", context, v)))
}

fn try_into_map(v: ValueReceive, context: &str) -> Result<HashMap<String, ValueReceive>> {
    v.try_into_map()
        .map_err(|v| Neo4jError::protocol_error(format!("{} was not map but {:?}", context, v)))
}
