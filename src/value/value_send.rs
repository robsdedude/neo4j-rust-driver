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

use super::spatial;
use super::time;
use super::value_receive::ValueReceive;
use super::ValueConversionError;
#[cfg(doc)]
use crate::error::Neo4jError;

/// For all temporal types: note that leap seconds are not supported and will result in a
/// [`Neo4jError::InvalidConfig`] when trying to be sent.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ValueSend {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<ValueSend>),
    Map(HashMap<String, ValueSend>),
    Cartesian2D(spatial::Cartesian2D),
    Cartesian3D(spatial::Cartesian3D),
    WGS84_2D(spatial::WGS84_2D),
    WGS84_3D(spatial::WGS84_3D),
    Duration(time::Duration),
    LocalTime(time::LocalTime),
    Time(time::Time),
    Date(time::Date),
    LocalDateTime(time::LocalDateTime),
    DateTime(time::DateTime),
    DateTimeFixed(time::DateTimeFixed),
}

macro_rules! impl_value_from_into {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for ValueSend {
                fn from(value: $ty) -> Self {
                    $value(value.into())
                }
            }
        )*
    };
}

macro_rules! impl_value_from_owned {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for ValueSend {
                fn from(value: $ty) -> Self {
                    $value(value)
                }
            }
        )*
    };
}

impl_value_from_into!(ValueSend::Boolean, bool);
impl_value_from_into!(ValueSend::Integer, u8, u16, u32, i8, i16, i32, i64);
impl_value_from_into!(ValueSend::Float, f32, f64);
impl_value_from_into!(ValueSend::String, &str);

impl_value_from_owned!(ValueSend::String, String);
// impl_value_from_owned!(Value::List, Vec<Value>);
// impl_value_from_owned!(Value::Map, HashMap<String, Value>);
impl_value_from_owned!(ValueSend::Cartesian2D, spatial::Cartesian2D);
impl_value_from_owned!(ValueSend::Cartesian3D, spatial::Cartesian3D);
impl_value_from_owned!(ValueSend::WGS84_2D, spatial::WGS84_2D);
impl_value_from_owned!(ValueSend::WGS84_3D, spatial::WGS84_3D);
impl_value_from_owned!(ValueSend::Duration, time::Duration);
impl_value_from_owned!(ValueSend::LocalTime, time::LocalTime);
impl_value_from_owned!(ValueSend::Time, time::Time);
impl_value_from_owned!(ValueSend::Date, time::Date);
impl_value_from_owned!(ValueSend::LocalDateTime, time::LocalDateTime);
impl_value_from_owned!(ValueSend::DateTime, time::DateTime);
impl_value_from_owned!(ValueSend::DateTimeFixed, time::DateTimeFixed);

impl<T: Into<ValueSend>> From<HashMap<String, T>> for ValueSend {
    fn from(value: HashMap<String, T>) -> Self {
        ValueSend::Map(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<ValueSend>> From<Vec<T>> for ValueSend {
    fn from(value: Vec<T>) -> Self {
        ValueSend::List(value.into_iter().map(|v| v.into()).collect())
    }
}

impl<T: Into<ValueSend>> From<Option<T>> for ValueSend {
    fn from(value: Option<T>) -> Self {
        match value {
            None => ValueSend::Null,
            Some(v) => v.into(),
        }
    }
}

impl TryFrom<ValueReceive> for ValueSend {
    type Error = ValueConversionError;

    fn try_from(v: ValueReceive) -> Result<Self, Self::Error> {
        Ok(match v {
            ValueReceive::Null => Self::Null,
            ValueReceive::Boolean(v) => Self::Boolean(v),
            ValueReceive::Integer(v) => Self::Integer(v),
            ValueReceive::Float(v) => Self::Float(v),
            ValueReceive::Bytes(v) => Self::Bytes(v),
            ValueReceive::String(v) => Self::String(v),
            ValueReceive::List(v) => Self::List(
                v.into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            ),
            ValueReceive::Map(v) => Self::Map(
                v.into_iter()
                    .map(|(k, e)| Ok::<_, Self::Error>((k, e.try_into()?)))
                    .collect::<Result<_, _>>()?,
            ),
            ValueReceive::Cartesian2D(v) => Self::Cartesian2D(v),
            ValueReceive::Cartesian3D(v) => Self::Cartesian3D(v),
            ValueReceive::WGS84_2D(v) => Self::WGS84_2D(v),
            ValueReceive::WGS84_3D(v) => Self::WGS84_3D(v),
            ValueReceive::Duration(v) => Self::Duration(v),
            ValueReceive::LocalTime(v) => Self::LocalTime(v),
            ValueReceive::Time(v) => Self::Time(v),
            ValueReceive::Date(v) => Self::Date(v),
            ValueReceive::LocalDateTime(v) => Self::LocalDateTime(v),
            ValueReceive::DateTime(v) => Self::DateTime(v),
            ValueReceive::DateTimeFixed(v) => Self::DateTimeFixed(v),
            ValueReceive::BrokenValue { .. } => return Err("cannot convert BrokenValue".into()),
            ValueReceive::Node(_) => return Err("cannot convert Node".into()),
            ValueReceive::Relationship(_) => return Err("cannot convert Relationship".into()),
            ValueReceive::Path(_) => return Err("cannot convert Path".into()),
        })
    }
}
