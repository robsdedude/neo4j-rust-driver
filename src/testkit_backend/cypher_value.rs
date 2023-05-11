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
use std::fmt::Formatter;
use std::num::FpCategory;

use serde::de::Unexpected;
use serde::{de::Error as DeError, de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::spatial::{Cartesian2D, Cartesian3D, WGS84_2D, WGS84_3D};
use crate::{ValueReceive, ValueSend};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "name", content = "data", deny_unknown_fields)]
pub(crate) enum CypherValue {
    CypherNull {
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<()>,
    },
    CypherList {
        value: Vec<CypherValue>,
    },
    CypherMap {
        value: HashMap<String, CypherValue>,
    },
    CypherInt {
        value: i64,
    },
    CypherBool {
        value: bool,
    },
    #[serde(
        serialize_with = "serialize_cypher_float",
        deserialize_with = "deserialize_cypher_float"
    )]
    CypherFloat {
        value: f64,
    },
    CypherString {
        value: String,
    },
    CypherBytes {
        #[serde(
            serialize_with = "serialize_cypher_bytes",
            deserialize_with = "deserialize_cypher_bytes"
        )]
        value: Vec<u8>,
    },
    Node(Node),
    CypherRelationship(Relationship),
    CypherPath {
        nodes: Vec<NodeTagged>,
        relationships: Vec<RelationshipTagged>,
    },
    CypherPoint {
        system: PointSystem,
        x: f64,
        y: f64,
        z: Option<f64>,
    },
    CypherDate {
        year: i64,
        month: i64,
        day: i64,
    },
    CypherTime {
        hour: i64,
        minute: i64,
        second: i64,
        nanosecond: i64,
        utc_offset_s: Option<i64>,
    },
    CypherDateTime {
        year: i64,
        month: i64,
        day: i64,
        hour: i64,
        minute: i64,
        second: i64,
        nanosecond: i64,
        utc_offset_s: Option<i64>,
        timezone_id: Option<String>,
    },
    CypherDuration {
        months: i64,
        days: i64,
        seconds: i64,
        nanoseconds: i64,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "name", content = "data", deny_unknown_fields)]
pub(crate) enum NodeTagged {
    Node(Node),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Node {
    id: i64,
    labels: Vec<String>,
    props: HashMap<String, CypherValue>,
    element_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "name", content = "data", deny_unknown_fields)]
pub(crate) enum RelationshipTagged {
    CypherRelationship(Relationship),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Relationship {
    id: i64,
    start_node_id: i64,
    end_node_id: i64,
    #[serde(rename = "type")]
    type_: String,
    props: HashMap<String, CypherValue>,
    element_id: String,
    start_node_element_id: String,
    end_node_element_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub(crate) enum PointSystem {
    Cartesian,
    WGS84,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn foo() {
        let r: Result<CypherValue, _> = serde_json::from_str(
            "{
    \"name\": \"CypherPath\",
    \"data\": {
        \"nodes\": [
            {
                \"name\": \"Node\",
                \"data\": {
                    \"id\": 1,
                    \"labels\": [],
                    \"props\": {},
                    \"elementId\": \"foobar\"
                }
            },
            {
                \"name\": \"Node\",
                \"data\": {
                    \"id\": 2,
                    \"labels\": [\"L1\"],
                    \"props\": {},
                    \"elementId\": \"baz\"
                }
            }
        ]
    }
}
        ",
        );
        let r = r.unwrap();
        println!("{r:?}");
    }
}

fn serialize_cypher_float<S>(v: &f64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let v = *v;
    match v.classify() {
        FpCategory::Nan => s.serialize_str("NaN"),
        FpCategory::Infinite => {
            if v < 0.0 {
                s.serialize_str("-Infinity")
            } else {
                s.serialize_str("+Infinity")
            }
        }
        _ => s.serialize_f64(v),
    }
}

fn deserialize_cypher_float<'de, D>(d: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_any(CypherFloatVisitor {})
}

struct CypherFloatVisitor {}

impl<'de> Visitor<'de> for CypherFloatVisitor {
    type Value = f64;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "number or one of \"+Infinity\", \"-Infinity\", \"NaN\""
        )
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(v)
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        match v {
            "+Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            "NaN" => Ok(f64::NAN),
            v => Err(E::invalid_value(Unexpected::Str(v), &self)),
        }
    }
}

fn serialize_cypher_bytes<S>(v: &[u8], s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if v.len() == 0 {
        return s.serialize_str("");
    }
    let mut repr = String::with_capacity(v.len() * 3 - 1);
    for b in &v[..v.len() - 1] {
        repr.push_str(&format!("{b:X} "));
    }
    repr.push_str(&format!("{:X}", &v[v.len() - 1]));
    s.serialize_str(&repr)
}

fn deserialize_cypher_bytes<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_str(CypherBytesVisitor {})
}

struct CypherBytesVisitor {}

impl<'de> Visitor<'de> for CypherBytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "hex encoded string representing bytes")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        v.split(' ')
            .filter(|s| !s.is_empty())
            .map(|s| u8::from_str_radix(s, 16).map_err(|e| E::custom(format!("{e}"))))
            .collect()
    }
}

#[derive(Error, Debug)]
#[error("{reason}")]
pub(crate) struct NotADriverValueError {
    reason: String,
}

impl TryFrom<CypherValue> for ValueSend {
    type Error = NotADriverValueError;

    fn try_from(v: CypherValue) -> Result<Self, Self::Error> {
        Ok(match v {
            CypherValue::CypherNull { .. } => ValueSend::Null,
            CypherValue::CypherList { value: v } => ValueSend::List(
                v.into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<_, _>>()?,
            ),
            CypherValue::CypherMap { value: v } => ValueSend::Map(
                v.into_iter()
                    .map(|(k, v)| Ok((k, v.try_into()?)))
                    .collect::<Result<_, _>>()?,
            ),
            CypherValue::CypherInt { value: v } => ValueSend::Integer(v),
            CypherValue::CypherBool { value: v } => ValueSend::Boolean(v),
            CypherValue::CypherFloat { value: v } => ValueSend::Float(v),
            CypherValue::CypherString { value: v } => ValueSend::String(v),
            CypherValue::CypherBytes { value: v } => ValueSend::Bytes(v),
            CypherValue::Node(_) => {
                return Err(NotADriverValueError {
                    reason: String::from("Nodes cannot be used as input type"),
                })
            }
            CypherValue::CypherRelationship(_) => {
                return Err(NotADriverValueError {
                    reason: String::from("Relationships cannot be used as input type"),
                })
            }
            CypherValue::CypherPath { .. } => {
                return Err(NotADriverValueError {
                    reason: String::from("Paths cannot be used as input type"),
                })
            }
            CypherValue::CypherPoint {
                system: PointSystem::Cartesian,
                x,
                y,
                z: None,
            } => Cartesian2D::new(x, y).into(),
            CypherValue::CypherPoint {
                system: PointSystem::Cartesian,
                x,
                y,
                z: Some(z),
            } => Cartesian3D::new(x, y, z).into(),
            CypherValue::CypherPoint {
                system: PointSystem::WGS84,
                x,
                y,
                z: None,
            } => WGS84_2D::new(x, y).into(),
            CypherValue::CypherPoint {
                system: PointSystem::WGS84,
                x,
                y,
                z: Some(z),
            } => WGS84_3D::new(x, y, z).into(),
            CypherValue::CypherDate { .. }
            | CypherValue::CypherTime { .. }
            | CypherValue::CypherDateTime { .. }
            | CypherValue::CypherDuration { .. } => {
                return Err(NotADriverValueError {
                    reason: String::from("Driver does not yet support temporal types"),
                })
            }
        })
    }
}

#[derive(Error, Debug)]
#[error("Record contains a broken value: {reason}")]
pub(crate) struct BrokenValueError {
    reason: String,
}

impl TryFrom<ValueReceive> for CypherValue {
    type Error = BrokenValueError;

    fn try_from(v: ValueReceive) -> Result<Self, Self::Error> {
        Ok(match v {
            ValueReceive::Null => CypherValue::CypherNull { value: None },
            ValueReceive::Boolean(v) => CypherValue::CypherBool { value: v },
            ValueReceive::Integer(v) => CypherValue::CypherInt { value: v },
            ValueReceive::Float(v) => CypherValue::CypherFloat { value: v },
            ValueReceive::Bytes(v) => CypherValue::CypherBytes { value: v },
            ValueReceive::String(v) => CypherValue::CypherString { value: v },
            ValueReceive::List(v) => CypherValue::CypherList {
                value: v
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            },
            ValueReceive::Map(v) => CypherValue::CypherMap {
                value: v
                    .into_iter()
                    .map(|(k, v)| Ok((k, v.try_into()?)))
                    .collect::<Result<_, _>>()?,
            },
            ValueReceive::Cartesian2D(v) => CypherValue::CypherPoint {
                system: PointSystem::Cartesian,
                x: v.x(),
                y: v.y(),
                z: None,
            },
            ValueReceive::Cartesian3D(v) => CypherValue::CypherPoint {
                system: PointSystem::Cartesian,
                x: v.x(),
                y: v.y(),
                z: Some(v.z()),
            },
            ValueReceive::WGS84_2D(v) => CypherValue::CypherPoint {
                system: PointSystem::WGS84,
                x: v.longitude(),
                y: v.latitude(),
                z: None,
            },
            ValueReceive::WGS84_3D(v) => CypherValue::CypherPoint {
                system: PointSystem::WGS84,
                x: v.longitude(),
                y: v.latitude(),
                z: Some(v.altitude()),
            },
            ValueReceive::BrokenValue { reason } => return Err(Self::Error { reason }),
        })
    }
}