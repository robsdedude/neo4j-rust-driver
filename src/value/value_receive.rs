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

use std::collections::{HashMap, VecDeque};

use itertools::Itertools;

use super::graph;
use super::spatial;
use super::value_send::ValueSend;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ValueReceive {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<ValueReceive>),
    Map(HashMap<String, ValueReceive>),
    Node(graph::Node),
    Relationship(graph::Relationship),
    Path(graph::Path),
    Cartesian2D(spatial::Cartesian2D),
    Cartesian3D(spatial::Cartesian3D),
    WGS84_2D(spatial::WGS84_2D),
    WGS84_3D(spatial::WGS84_3D),
    BrokenValue(BrokenValue),
}

impl ValueReceive {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, ValueReceive::Null)
    }
}

impl TryFrom<ValueReceive> for bool {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Boolean(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, ValueReceive::Boolean(_))
    }

    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ValueReceive::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bool(self) -> Result<bool, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for i64 {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Integer(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, ValueReceive::Integer(_))
    }

    #[inline]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            ValueReceive::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_int(self) -> Result<i64, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for f64 {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Float(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, ValueReceive::Float(_))
    }

    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            ValueReceive::Float(v) => Some(*v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_float(self) -> Result<f64, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for Vec<u8> {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Bytes(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, ValueReceive::Bytes(_))
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            ValueReceive::Bytes(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_bytes(self) -> Result<Vec<u8>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for String {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::String(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, ValueReceive::String(_))
    }

    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            ValueReceive::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_string(self) -> Result<String, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for Vec<ValueReceive> {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::List(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, ValueReceive::List(_))
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[ValueReceive]> {
        match self {
            ValueReceive::List(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_list(self) -> Result<Vec<ValueReceive>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for HashMap<String, ValueReceive> {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Map(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, ValueReceive::Map(_))
    }

    #[inline]
    pub fn as_map(&self) -> Option<&HashMap<String, ValueReceive>> {
        match self {
            ValueReceive::Map(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_map(self) -> Result<HashMap<String, ValueReceive>, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for graph::Node {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Node(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_node(&self) -> bool {
        matches!(self, ValueReceive::Node(_))
    }

    #[inline]
    pub fn as_node(&self) -> Option<&graph::Node> {
        match self {
            ValueReceive::Node(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_node(self) -> Result<graph::Node, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for graph::Relationship {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Relationship(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_relationship(&self) -> bool {
        matches!(self, ValueReceive::Relationship(_))
    }

    #[inline]
    pub fn as_relationship(&self) -> Option<&graph::Relationship> {
        match self {
            ValueReceive::Relationship(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_relationship(self) -> Result<graph::Relationship, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for graph::Path {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Path(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_path(&self) -> bool {
        matches!(self, ValueReceive::Path(_))
    }

    #[inline]
    pub fn as_path(&self) -> Option<&graph::Path> {
        match self {
            ValueReceive::Path(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_path(self) -> Result<graph::Path, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::Cartesian2D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Cartesian2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_cartesian_2d(&self) -> bool {
        matches!(self, ValueReceive::Cartesian2D(_))
    }

    #[inline]
    pub fn as_cartesian_2d(&self) -> Option<&spatial::Cartesian2D> {
        match self {
            ValueReceive::Cartesian2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_cartesian_2d(self) -> Result<spatial::Cartesian2D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::Cartesian3D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::Cartesian3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_cartesian_3d(&self) -> bool {
        matches!(self, ValueReceive::Cartesian3D(_))
    }

    #[inline]
    pub fn as_cartesian_3d(&self) -> Option<&spatial::Cartesian3D> {
        match self {
            ValueReceive::Cartesian3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_cartesian_3d(self) -> Result<spatial::Cartesian3D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::WGS84_2D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::WGS84_2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_wgs84_2d(&self) -> bool {
        matches!(self, ValueReceive::WGS84_2D(_))
    }

    #[inline]
    pub fn as_wgs84_2d(&self) -> Option<&spatial::WGS84_2D> {
        match self {
            ValueReceive::WGS84_2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_wgs84_2d(self) -> Result<spatial::WGS84_2D, Self> {
        self.try_into()
    }
}

impl TryFrom<ValueReceive> for spatial::WGS84_3D {
    type Error = ValueReceive;

    #[inline]
    fn try_from(value: ValueReceive) -> Result<Self, Self::Error> {
        match value {
            ValueReceive::WGS84_3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl ValueReceive {
    #[inline]
    pub fn is_wgs84_3d(&self) -> bool {
        matches!(self, ValueReceive::WGS84_3D(_))
    }

    #[inline]
    pub fn as_wgs84_3d(&self) -> Option<&spatial::WGS84_3D> {
        match self {
            ValueReceive::WGS84_3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_wgs84_3d(self) -> Result<spatial::WGS84_3D, Self> {
        self.try_into()
    }
}

#[derive(Debug, Clone)]
pub struct BrokenValue {
    pub(crate) inner: BrokenValueInner,
}

impl PartialEq for BrokenValue {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BrokenValueInner {
    Reason(String),
    UnknownStruct {
        tag: u8,
        fields: VecDeque<ValueReceive>,
    },
    InvalidStruct {
        reason: String,
    },
}

impl BrokenValue {
    pub fn reason(&self) -> &str {
        match &self.inner {
            BrokenValueInner::Reason(reason) => reason,
            BrokenValueInner::UnknownStruct { .. } => "received an unknown packstream struct",
            BrokenValueInner::InvalidStruct { reason, .. } => reason,
        }
    }
}

impl From<BrokenValueInner> for BrokenValue {
    fn from(inner: BrokenValueInner) -> Self {
        BrokenValue { inner }
    }
}

pub(crate) fn broken_value(reason: impl Into<String>) -> ValueReceive {
    ValueReceive::BrokenValue(BrokenValue {
        inner: BrokenValueInner::Reason(reason.into()),
    })
}

impl ValueReceive {
    pub(crate) fn dbg_print(&self) -> String {
        match self {
            ValueReceive::Null => "null".into(),
            ValueReceive::Boolean(v) => format!("{}", v),
            ValueReceive::Integer(v) => format!("{}", v),
            ValueReceive::Float(v) => format!("{}", v),
            ValueReceive::Bytes(v) => format!("bytes{:02X?}", v),
            ValueReceive::String(v) => format!("{:?}", v),
            ValueReceive::List(v) => format!("[{}]", v.iter().map(|e| e.dbg_print()).format(", ")),
            ValueReceive::Map(v) => format!(
                "{{{}}}",
                v.iter()
                    .map(|(k, e)| format!("{:?}: {}", k, e.dbg_print()))
                    .format(", ")
            ),
            ValueReceive::Node(node) => format!("{}", node),
            ValueReceive::Relationship(relationship) => format!("{}", relationship),
            ValueReceive::Path(path) => format!("{}", path),
            ValueReceive::Cartesian2D(v) => format!("{:?}", v),
            ValueReceive::Cartesian3D(v) => format!("{:?}", v),
            ValueReceive::WGS84_2D(v) => format!("{:?}", v),
            ValueReceive::WGS84_3D(v) => format!("{:?}", v),
            ValueReceive::BrokenValue(broken_value) => {
                format!("BrokenValue({})", broken_value.reason())
            }
        }
    }
}

impl From<ValueSend> for ValueReceive {
    fn from(v: ValueSend) -> Self {
        match v {
            ValueSend::Null => Self::Null,
            ValueSend::Boolean(v) => Self::Boolean(v),
            ValueSend::Integer(v) => Self::Integer(v),
            ValueSend::Float(v) => Self::Float(v),
            ValueSend::Bytes(v) => Self::Bytes(v),
            ValueSend::String(v) => Self::String(v),
            ValueSend::List(v) => Self::List(v.into_iter().map(Into::into).collect()),
            ValueSend::Map(v) => Self::Map(v.into_iter().map(|(k, e)| (k, e.into())).collect()),
            ValueSend::Cartesian2D(v) => Self::Cartesian2D(v),
            ValueSend::Cartesian3D(v) => Self::Cartesian3D(v),
            ValueSend::WGS84_2D(v) => Self::WGS84_2D(v),
            ValueSend::WGS84_3D(v) => Self::WGS84_3D(v),
        }
    }
}
