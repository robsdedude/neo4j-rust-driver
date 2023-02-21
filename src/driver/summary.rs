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

use super::io::bolt::BoltMeta;
use crate::{Neo4jError, Result, Value};

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Summary {
    pub result_available_after: i64,
    pub result_consumed_after: i64,
    pub counters: SummaryCounters,
    // pub notifications: Vec<...>,
    // pub profile: Option<...>,
    // pub plan: Option<...>,
    pub query_type: SummaryQueryType,
    pub database: Option<String>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SummaryCounters {}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum SummaryQueryType {
    #[default]
    Read,
    Write,
    ReadWrite,
    Schema,
}

impl Summary {
    pub(crate) fn load_run_meta(&mut self, meta: &mut BoltMeta) -> Result<()> {
        if let Some(t_first) = meta.remove("t_first") {
            let Value::Integer(t_first) = t_first else {
                return Err(Neo4jError::ProtocolError {
                    message: format!("t_first in summary was not integer but {:?}",
                                     t_first),
                })
            };
            self.result_available_after = t_first;
        }
        Ok(())
    }
    pub(crate) fn load_pull_meta(&mut self, meta: &mut BoltMeta) -> Result<()> {
        if let Some(t_last) = meta.remove("t_last") {
            let Value::Integer(t_last) = t_last else {
                return Err(Neo4jError::ProtocolError {
                    message: format!("t_last in summary was not integer but {:?}", t_last),
                })
            };
            self.result_consumed_after = t_last;
        }
        if let Some(db) = meta.remove("db") {
            let Value::String(db) = db else {
                return Err(Neo4jError::ProtocolError {
                    message: format!("db in summary was not string but {:?}", db),
                })
            };
            self.database = Some(db);
        }
        if let Some(query_type) = meta.remove("type") {
            let Value::String(query_type) = query_type else {
                return Err(Neo4jError::ProtocolError {
                    message: format!("type in summary was not string but {:?}", query_type),
                })
            };
            self.query_type = match query_type.as_str() {
                "r" => SummaryQueryType::Read,
                "w" => SummaryQueryType::Write,
                "rw" => SummaryQueryType::ReadWrite,
                "s" => SummaryQueryType::Schema,
                _ => {
                    return Err(Neo4jError::ProtocolError {
                        message: format!("type in summary was an unknown string {:?}", query_type),
                    })
                }
            };
        } else {
            return Err(Neo4jError::ProtocolError {
                message: "type in summary missing".into(),
            });
        }
        Ok(())
    }
}
