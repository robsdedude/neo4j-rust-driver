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
use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::num::FpCategory;
use std::str::FromStr;

use chrono::{Datelike, Offset, TimeZone, Timelike};
use chrono_0_4 as chrono;
use chrono_tz_0_10 as chrono_tz;
use serde::de::Unexpected;
use serde::{de::Error as DeError, de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue, Value};
use thiserror::Error;

use super::errors::TestKitError;
use neo4j::driver::Record;
use neo4j::value::graph::{
    Node as Neo4jNode, Relationship as Neo4jRelationship, RelationshipDirection,
    UnboundRelationship as Neo4jUnboundRelationship,
};
use neo4j::value::spatial::{Cartesian2D, Cartesian3D, WGS84_2D, WGS84_3D};
use neo4j::value::{time, ValueReceive, ValueSend};

#[derive(Debug)]
#[repr(transparent)]
pub(super) struct ConvertableJsonValue(pub(super) JsonValue);

#[derive(Debug)]
#[repr(transparent)]
pub(super) struct ConvertableValueSend(pub(super) ValueSend);

#[derive(Debug)]
#[repr(transparent)]
pub(super) struct ConvertableValueReceive(pub(super) ValueReceive);

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "name", content = "data", deny_unknown_fields)]
pub(super) enum CypherValue {
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
    CypherFloat {
        #[serde(
            serialize_with = "serialize_cypher_float",
            deserialize_with = "deserialize_cypher_float"
        )]
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
    Node(CypherNode),
    CypherRelationship(CypherRelationship),
    CypherPath {
        nodes: Box<CypherValue>,
        relationships: Box<CypherValue>,
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
    CypherDateTime(CypherDateTime),
    CypherDuration {
        months: i64,
        days: i64,
        seconds: i64,
        nanoseconds: i64,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct CypherNode {
    id: Box<CypherValue>,
    labels: Box<CypherValue>,
    props: Box<CypherValue>,
    element_id: Box<CypherValue>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct CypherRelationship {
    id: Box<CypherValue>,
    start_node_id: Box<CypherValue>,
    end_node_id: Box<CypherValue>,
    #[serde(rename = "type")]
    type_: Box<CypherValue>,
    props: Box<CypherValue>,
    element_id: Box<CypherValue>,
    start_node_element_id: Box<CypherValue>,
    end_node_element_id: Box<CypherValue>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CypherDateTime {
    pub(super) year: i64,
    pub(super) month: i64,
    pub(super) day: i64,
    pub(super) hour: i64,
    pub(super) minute: i64,
    pub(super) second: i64,
    pub(super) nanosecond: i64,
    pub(super) utc_offset_s: Option<i64>,
    pub(super) timezone_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub(super) enum PointSystem {
    Cartesian,
    WGS84,
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

impl Visitor<'_> for CypherFloatVisitor {
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

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(v as f64)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(v as f64)
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
    if v.is_empty() {
        return s.serialize_str("");
    }
    let mut repr = String::with_capacity(v.len() * 3 - 1);
    for b in &v[..v.len() - 1] {
        repr.push_str(&format!("{b:02x} "));
    }
    repr.push_str(&format!("{:02x}", &v[v.len() - 1]));
    s.serialize_str(&repr)
}

fn deserialize_cypher_bytes<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    d.deserialize_str(CypherBytesVisitor {})
}

struct CypherBytesVisitor {}

impl Visitor<'_> for CypherBytesVisitor {
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
pub(super) struct NotADriverValueError {
    reason: String,
}

impl NotADriverValueError {
    fn new<S: Into<String>>(reason: S) -> Self {
        NotADriverValueError {
            reason: reason.into(),
        }
    }
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
                return Err(NotADriverValueError::new(
                    "Nodes cannot be used as input type",
                ))
            }
            CypherValue::CypherRelationship(_) => {
                return Err(NotADriverValueError::new(
                    "Relationships cannot be used as input type",
                ))
            }
            CypherValue::CypherPath { .. } => {
                return Err(NotADriverValueError::new(
                    "Paths cannot be used as input type",
                ))
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
            CypherValue::CypherDate { year, month, day } => {
                let year = try_from_value(year, "CypherDate", "year")?;
                let month = try_from_value(month, "CypherDate", "month")?;
                let day = try_from_value(day, "CypherDate", "day")?;
                time::Date::from_components(time::DateComponents::from_ymd(year, month, day))
                    .ok_or_else(|| NotADriverValueError::new("CypherDate is out of range"))?
                    .into()
            }
            CypherValue::CypherTime {
                hour,
                minute,
                second,
                nanosecond,
                utc_offset_s,
            } => {
                let hour = try_from_value(hour, "CypherTime", "hour")?;
                let minute = try_from_value(minute, "CypherTime", "minute")?;
                let second = try_from_value(second, "CypherTime", "second")?;
                let nanosecond = try_from_value(nanosecond, "CypherTime", "nanosecond")?;
                let components =
                    time::TimeComponents::from_hms_nano(hour, minute, second, nanosecond);
                let Some(utc_offset_s) = utc_offset_s else {
                    let time = time::LocalTime::from_components(components)
                        .ok_or_else(|| NotADriverValueError::new("CypherTime is out of range"))?;
                    return Ok(time.into());
                };
                let utc_offset_s = try_from_value(utc_offset_s, "CypherTime", "utc_offset_s")?;
                time::Time::from_utc_components(components, utc_offset_s)
                    .ok_or_else(|| NotADriverValueError::new("CypherTime is out of range"))?
                    .into()
            }
            CypherValue::CypherDateTime(date_time) => cypher_date_time_to_value_send(date_time)?,
            CypherValue::CypherDuration {
                months,
                days,
                seconds,
                nanoseconds,
            } => time::Duration::new(
                months,
                days,
                seconds,
                try_from_value(nanoseconds, "CypherDuration", "nanoseconds")?,
            )
            .into(),
        })
    }
}

fn cypher_date_time_to_value_send(
    date_time: CypherDateTime,
) -> Result<ValueSend, NotADriverValueError> {
    let CypherDateTime {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        utc_offset_s,
        timezone_id,
    } = date_time;

    let naive_chrono_dt = || {
        let year = try_from_value(year, "CypherDateTime", "year")?;
        let month = try_from_value(month, "CypherDateTime", "month")?;
        let day = try_from_value(day, "CypherDateTime", "day")?;
        let hour = try_from_value(hour, "CypherDateTime", "hour")?;
        let minute = try_from_value(minute, "CypherDateTime", "minute")?;
        let second = try_from_value(second, "CypherDateTime", "second")?;
        let nanosecond = try_from_value(nanosecond, "CypherDateTime", "nanosecond")?;
        let dt = chrono::NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|dt| dt.and_hms_nano_opt(hour, minute, second, nanosecond))
            .ok_or_else(|| NotADriverValueError {
                reason: String::from("CypherDateTime is out of range"),
            })?;
        Ok(dt)
    };

    match (utc_offset_s, timezone_id) {
        (None, None) => {
            // LocalDateTime
            Ok(time::LocalDateTime::try_from(naive_chrono_dt()?)
                .map_err(|_| NotADriverValueError::new("CypherDateTime is out of range"))?
                .into())
        }
        (Some(utc_offset_s), None) => {
            // DateTimeFixed
            let utc_offset_s: i32 = try_from_value(utc_offset_s, "CypherDateTime", "utc_offset_s")?;
            let tz = chrono::FixedOffset::east_opt(utc_offset_s).ok_or_else(|| {
                NotADriverValueError::new("CypherDateTime utc_offset_s is invalid")
            })?;
            let dt = naive_chrono_dt()?;
            let dt = tz.from_local_datetime(&dt).single().ok_or_else(|| {
                NotADriverValueError::new("CypherDateTime non-existent local date time")
            })?;

            let dt = time::DateTimeFixed::from_chrono_0_4(&dt)
                .ok_or_else(|| NotADriverValueError::new("CypherDateTime is out of range"))?;
            Ok(dt.into())
        }
        (Some(utc_offset_s), Some(timezone_id)) => {
            let utc_offset_s: i32 = try_from_value(utc_offset_s, "CypherDateTime", "utc_offset_s")?;
            let tz = chrono_tz::Tz::from_str(&timezone_id)
                .map_err(|_| NotADriverValueError::new("CypherDateTime timezone_id is invalid"))?;
            let dt = match tz.from_local_datetime(&naive_chrono_dt()?) {
                chrono::LocalResult::None => None,
                chrono::LocalResult::Single(dt) => {
                    if dt.offset().fix().local_minus_utc() == utc_offset_s {
                        Some(dt)
                    } else {
                        return Err(NotADriverValueError::new(
                            "CypherDateTime with given utc offset doesn't exist",
                        ));
                    }
                }
                chrono::LocalResult::Ambiguous(dt1, dt2) => {
                    if dt1.offset().fix().local_minus_utc() == utc_offset_s {
                        Some(dt1)
                    } else if dt2.offset().fix().local_minus_utc() == utc_offset_s {
                        Some(dt2)
                    } else {
                        return Err(NotADriverValueError::new(
                            "CypherDateTime with given utc offset doesn't exist",
                        ));
                    }
                }
            }
            .ok_or_else(|| {
                NotADriverValueError::new("CypherDateTime non-existent local date time")
            })?;
            let dt = time::DateTime::from_chrono_0_4_chrono_tz_0_10(&dt)
                .ok_or_else(|| NotADriverValueError::new("CypherDateTime is out of range"))?;
            Ok(dt.into())
        }
        (None, Some(_)) => Err(NotADriverValueError::new(
            "CypherDateTime timezone_id requires utc_offset_s to be present",
        )),
    }
}

fn try_from_value<R>(
    value: impl TryInto<R> + Display + Copy,
    struct_name: &str,
    value_name: &str,
) -> Result<R, NotADriverValueError> {
    value.try_into().map_err(|_| NotADriverValueError {
        reason: format!("{struct_name} {value_name} {value} is out of range"),
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[repr(transparent)]
#[serde(transparent)]
pub(super) struct CypherValues(pub(super) Vec<CypherValue>);

impl TryFrom<Record> for CypherValues {
    type Error = TestKitError;

    fn try_from(record: Record) -> Result<Self, Self::Error> {
        Ok(CypherValues(
            record
                .into_values()
                .map(|v| v.try_into())
                .collect::<Result<_, _>>()?,
        ))
    }
}

impl FromIterator<CypherValue> for CypherValues {
    fn from_iter<T: IntoIterator<Item = CypherValue>>(iter: T) -> Self {
        CypherValues(iter.into_iter().collect())
    }
}

impl TryFrom<ValueSend> for CypherValue {
    type Error = TestKitError;

    fn try_from(v: ValueSend) -> Result<Self, Self::Error> {
        Ok(match v {
            ValueSend::Null => CypherValue::CypherNull { value: None },
            ValueSend::Boolean(value) => CypherValue::CypherBool { value },
            ValueSend::Integer(value) => CypherValue::CypherInt { value },
            ValueSend::Float(value) => CypherValue::CypherFloat { value },
            ValueSend::Bytes(value) => CypherValue::CypherBytes { value },
            ValueSend::String(value) => CypherValue::CypherString { value },
            ValueSend::List(value) => CypherValue::CypherList {
                value: try_into_vec(value)?,
            },
            ValueSend::Map(value) => CypherValue::CypherMap {
                value: try_into_hash_map(value)?,
            },
            ValueSend::Cartesian2D(value) => CypherValue::CypherPoint {
                system: PointSystem::Cartesian,
                x: value.x(),
                y: value.y(),
                z: None,
            },
            ValueSend::Cartesian3D(value) => CypherValue::CypherPoint {
                system: PointSystem::Cartesian,
                x: value.x(),
                y: value.y(),
                z: Some(value.z()),
            },
            ValueSend::WGS84_2D(value) => CypherValue::CypherPoint {
                system: PointSystem::WGS84,
                x: value.longitude(),
                y: value.longitude(),
                z: None,
            },
            ValueSend::WGS84_3D(value) => CypherValue::CypherPoint {
                system: PointSystem::WGS84,
                x: value.longitude(),
                y: value.longitude(),
                z: Some(value.altitude()),
            },
            ValueSend::Duration(value) => duration_to_cypher_value(value),
            ValueSend::LocalTime(value) => local_time_to_cypher_value(value),
            ValueSend::Time(value) => time_to_cypher_value(value),
            ValueSend::Date(value) => date_to_cypher_value(value)?,
            ValueSend::LocalDateTime(value) => local_date_time_to_cypher_value(value)?,
            ValueSend::DateTime(value) => date_time_to_cypher_value(value)?,
            ValueSend::DateTimeFixed(value) => date_time_fixed_to_cypher_value(value)?,
            _ => {
                return Err(TestKitError::backend_err(format!(
                    "Failed to serialize ValueSend to json: {v:?}",
                )))
            }
        })
    }
}

impl From<ConvertableJsonValue> for ValueSend {
    fn from(value: ConvertableJsonValue) -> Self {
        match value.0 {
            Value::Null => ValueSend::Null,
            Value::Bool(v) => v.into(),
            Value::Number(n) => match n.as_i64() {
                Some(n) => ValueSend::Integer(n),
                None => ValueSend::Float(n.as_f64().unwrap()),
            },
            Value::String(s) => ValueSend::String(s),
            Value::Array(a) => ValueSend::List(
                a.into_iter()
                    .map(|v| ConvertableJsonValue(v).into())
                    .collect(),
            ),
            Value::Object(o) => ValueSend::Map(
                o.into_iter()
                    .map(|(k, v)| (k, ConvertableJsonValue(v).into()))
                    .collect::<HashMap<_, _>>(),
            ),
        }
    }
}

impl TryFrom<ConvertableValueSend> for JsonValue {
    type Error = String;

    fn try_from(v: ConvertableValueSend) -> Result<Self, Self::Error> {
        Ok(match v.0 {
            ValueSend::Null => JsonValue::Null,
            ValueSend::Boolean(v) => JsonValue::Bool(v),
            ValueSend::Integer(v) => JsonValue::Number(v.into()),
            ValueSend::Float(v) => JsonValue::Number(
                JsonNumber::from_f64(v).ok_or(format!("Failed to serialize float: {v}"))?,
            ),
            ValueSend::String(v) => JsonValue::String(v),
            ValueSend::List(v) => {
                JsonValue::Array(try_into_vec(v.into_iter().map(ConvertableValueSend))?)
            }
            ValueSend::Map(v) => JsonValue::Object(try_into_json_map(
                v.into_iter().map(|(k, v)| (k, ConvertableValueSend(v))),
            )?),
            _ => return Err(format!("Failed to serialize to json: {v:?}")),
        })
    }
}

#[derive(Error, Debug)]
#[error("record contains a broken value: {reason}")]
pub(super) struct BrokenValueError {
    reason: String,
}

impl TryFrom<ValueReceive> for CypherValue {
    type Error = TestKitError;

    fn try_from(v: ValueReceive) -> Result<Self, Self::Error> {
        Ok(match v {
            ValueReceive::Null => CypherValue::CypherNull { value: None },
            ValueReceive::Boolean(v) => CypherValue::CypherBool { value: v },
            ValueReceive::Integer(v) => CypherValue::CypherInt { value: v },
            ValueReceive::Float(v) => CypherValue::CypherFloat { value: v },
            ValueReceive::Bytes(v) => CypherValue::CypherBytes { value: v },
            ValueReceive::String(v) => CypherValue::CypherString { value: v },
            ValueReceive::List(v) => CypherValue::CypherList {
                value: try_into_vec(v)?,
            },
            ValueReceive::Map(v) => CypherValue::CypherMap {
                value: try_into_hash_map(v)?,
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
            ValueReceive::Node(n) => CypherValue::Node(try_into_node(n)?),
            ValueReceive::Relationship(r) => {
                CypherValue::CypherRelationship(try_into_relationship(r)?)
            }
            ValueReceive::Path(p) => {
                let (mut start_node, hops) = p.traverse();
                let mut nodes = Vec::with_capacity(hops.len() + 1);
                let mut relationships = Vec::with_capacity(hops.len());
                nodes.push(CypherValue::Node(try_into_node(start_node.clone())?));
                for (direction, relationship, end_node) in hops {
                    nodes.push(CypherValue::Node(try_into_node(end_node.clone())?));
                    let relationship = relationship.clone();
                    relationships.push(CypherValue::CypherRelationship(match direction {
                        RelationshipDirection::To => {
                            try_into_relationship_unbound(start_node, relationship, end_node)?
                        }
                        RelationshipDirection::From => {
                            try_into_relationship_unbound(end_node, relationship, start_node)?
                        }
                    }));
                    start_node = end_node;
                }
                CypherValue::CypherPath {
                    nodes: Box::new(CypherValue::CypherList { value: nodes }),
                    relationships: Box::new(CypherValue::CypherList {
                        value: relationships,
                    }),
                }
            }
            ValueReceive::Duration(value) => duration_to_cypher_value(value),
            ValueReceive::LocalTime(value) => local_time_to_cypher_value(value),
            ValueReceive::Time(value) => time_to_cypher_value(value),
            ValueReceive::Date(value) => date_to_cypher_value(value)?,
            ValueReceive::LocalDateTime(value) => local_date_time_to_cypher_value(value)?,
            ValueReceive::DateTime(value) => date_time_to_cypher_value(value)?,
            ValueReceive::DateTimeFixed(value) => date_time_fixed_to_cypher_value(value)?,
            ValueReceive::BrokenValue(v) => {
                return Err(BrokenValueError {
                    reason: v.reason().into(),
                }
                .into())
            }
            _ => {
                return Err(TestKitError::backend_err(format!(
                    "Failed to serialize ValueReceive to json: {v:?}",
                )))
            }
        })
    }
}

fn duration_to_cypher_value(value: time::Duration) -> CypherValue {
    CypherValue::CypherDuration {
        months: value.months(),
        days: value.days(),
        seconds: value.seconds(),
        nanoseconds: value.nanoseconds().into(),
    }
}

fn local_time_to_cypher_value(value: time::LocalTime) -> CypherValue {
    let components = value.to_components();
    CypherValue::CypherTime {
        hour: components.hour.into(),
        minute: components.min.into(),
        second: components.sec.into(),
        nanosecond: components.nano.into(),
        utc_offset_s: None,
    }
}

fn time_to_cypher_value(value: time::Time) -> CypherValue {
    let (components, utc_offset_s) = value.to_utc_components();
    CypherValue::CypherTime {
        hour: components.hour.into(),
        minute: components.min.into(),
        second: components.sec.into(),
        nanosecond: components.nano.into(),
        utc_offset_s: Some(utc_offset_s.into()),
    }
}

fn date_to_cypher_value(value: time::Date) -> Result<CypherValue, TestKitError> {
    let value = value.to_chrono_0_4().ok_or_else(|| {
        TestKitError::backend_err(format!(
            "Date {value:?} cannot be serialized because it's out of range for chrono"
        ))
    })?;
    Ok(CypherValue::CypherDate {
        year: value.year().into(),
        month: value.month().into(),
        day: value.day().into(),
    })
}

fn local_date_time_to_cypher_value(
    value: time::LocalDateTime,
) -> Result<CypherValue, TestKitError> {
    let value = value.to_chrono_0_4().ok_or_else(|| {
        TestKitError::backend_err(format!(
            "LocalDateTime {value:?} cannot be serialized because it's out of range for chrono"
        ))
    })?;
    Ok(CypherValue::CypherDateTime(CypherDateTime {
        year: value.year().into(),
        month: value.month().into(),
        day: value.day().into(),
        hour: value.hour().into(),
        minute: value.minute().into(),
        second: value.second().into(),
        nanosecond: value.nanosecond().into(),
        utc_offset_s: None,
        timezone_id: None,
    }))
}

fn date_time_to_cypher_value(value: time::DateTime) -> Result<CypherValue, TestKitError> {
    let value = value.to_chrono_0_4_chrono_tz_0_10().ok_or_else(|| {
        TestKitError::backend_err(format!(
            "DateTime {value:?} cannot be serialized because it's out of range for chrono"
        ))
    })?;
    Ok(CypherValue::CypherDateTime(CypherDateTime {
        year: value.year().into(),
        month: value.month().into(),
        day: value.day().into(),
        hour: value.hour().into(),
        minute: value.minute().into(),
        second: value.second().into(),
        nanosecond: value.nanosecond().into(),
        utc_offset_s: Some(value.offset().fix().local_minus_utc().into()),
        timezone_id: Some(value.timezone().name().into()),
    }))
}

fn date_time_fixed_to_cypher_value(
    value: time::DateTimeFixed,
) -> Result<CypherValue, TestKitError> {
    let value = value.to_chrono_0_4().ok_or_else(|| {
        TestKitError::backend_err(format!(
            "DateTimeFixed {value:?} cannot be serialized because it's out of range for chrono"
        ))
    })?;
    Ok(CypherValue::CypherDateTime(CypherDateTime {
        year: value.year().into(),
        month: value.month().into(),
        day: value.day().into(),
        hour: value.hour().into(),
        minute: value.minute().into(),
        second: value.second().into(),
        nanosecond: value.nanosecond().into(),
        utc_offset_s: Some(value.offset().fix().local_minus_utc().into()),
        timezone_id: None,
    }))
}

#[allow(clippy::result_large_err)]
fn try_into_node(n: Neo4jNode) -> Result<CypherNode, TestKitError> {
    Ok(CypherNode {
        id: Box::new(CypherValue::CypherInt { value: n.id }),
        labels: Box::new(CypherValue::CypherList {
            value: n
                .labels
                .into_iter()
                .map(|l| CypherValue::CypherString { value: l })
                .collect(),
        }),
        props: Box::new(CypherValue::CypherMap {
            value: try_into_hash_map(n.properties)?,
        }),
        element_id: Box::new(CypherValue::CypherString {
            value: n.element_id,
        }),
    })
}

#[allow(clippy::result_large_err)]
fn try_into_relationship(r: Neo4jRelationship) -> Result<CypherRelationship, TestKitError> {
    Ok(CypherRelationship {
        id: Box::new(CypherValue::CypherInt { value: r.id }),
        start_node_id: Box::new(CypherValue::CypherInt {
            value: r.start_node_id,
        }),
        end_node_id: Box::new(CypherValue::CypherInt {
            value: r.end_node_id,
        }),
        type_: Box::new(CypherValue::CypherString { value: r.type_ }),
        props: Box::new(CypherValue::CypherMap {
            value: try_into_hash_map(r.properties)?,
        }),

        element_id: Box::new(CypherValue::CypherString {
            value: r.element_id,
        }),
        start_node_element_id: Box::new(CypherValue::CypherString {
            value: r.start_node_element_id,
        }),
        end_node_element_id: Box::new(CypherValue::CypherString {
            value: r.end_node_element_id,
        }),
    })
}

#[allow(clippy::result_large_err)]
fn try_into_relationship_unbound(
    s: &Neo4jNode,
    r: Neo4jUnboundRelationship,
    e: &Neo4jNode,
) -> Result<CypherRelationship, TestKitError> {
    Ok(CypherRelationship {
        id: Box::new(CypherValue::CypherInt { value: r.id }),
        start_node_id: Box::new(CypherValue::CypherInt { value: s.id }),
        end_node_id: Box::new(CypherValue::CypherInt { value: e.id }),
        type_: Box::new(CypherValue::CypherString { value: r.type_ }),
        props: Box::new(CypherValue::CypherMap {
            value: try_into_hash_map(r.properties)?,
        }),
        element_id: Box::new(CypherValue::CypherString {
            value: r.element_id,
        }),
        start_node_element_id: Box::new(CypherValue::CypherString {
            value: s.element_id.clone(),
        }),
        end_node_element_id: Box::new(CypherValue::CypherString {
            value: e.element_id.clone(),
        }),
    })
}

fn try_into_vec<T: TryFrom<V, Error = E>, E, V>(
    v: impl IntoIterator<Item = V>,
) -> Result<Vec<T>, E> {
    v.into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()
}

fn try_into_hash_map<T: TryFrom<V, Error = E>, E, K: Eq + Hash, V>(
    v: impl IntoIterator<Item = (K, V)>,
) -> Result<HashMap<K, T>, E> {
    v.into_iter()
        .map(|(k, v)| Ok((k, v.try_into()?)))
        .collect::<Result<_, _>>()
}

fn try_into_json_map<E, V: TryInto<JsonValue, Error = E>>(
    v: impl IntoIterator<Item = (String, V)>,
) -> Result<JsonMap<String, JsonValue>, E> {
    v.into_iter()
        .map(|(k, v)| Ok((k, v.try_into()?)))
        .collect::<Result<_, _>>()
}

impl TryFrom<ConvertableValueReceive> for JsonValue {
    type Error = String;

    fn try_from(v: ConvertableValueReceive) -> Result<Self, Self::Error> {
        Ok(match v.0 {
            ValueReceive::Null => JsonValue::Null,
            ValueReceive::Boolean(v) => JsonValue::Bool(v),
            ValueReceive::Integer(v) => JsonValue::Number(v.into()),
            ValueReceive::Float(v) => JsonValue::Number(
                JsonNumber::from_f64(v).ok_or(format!("Failed to serialize float: {v}"))?,
            ),
            ValueReceive::String(v) => JsonValue::String(v),
            ValueReceive::List(v) => {
                JsonValue::Array(try_into_vec(v.into_iter().map(ConvertableValueReceive))?)
            }
            ValueReceive::Map(v) => JsonValue::Object(try_into_json_map(
                v.into_iter().map(|(k, v)| (k, ConvertableValueReceive(v))),
            )?),
            _ => return Err(format!("Failed to serialize to json: {v:?}")),
        })
    }
}
