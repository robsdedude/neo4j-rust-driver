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

// mod de;
// mod ser;

pub mod spatial;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Cartesian2D(spatial::Cartesian2D),
    Cartesian3D(spatial::Cartesian3D),
    WGS84_2D(spatial::WGS84_2D),
    WGS84_3D(spatial::WGS84_3D),
    BrokenValue { reason: String },
}

impl Value {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl TryFrom<Value> for bool {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Boolean(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Boolean(_))
    }

    #[inline]
    pub fn as_bool(&self) -> Option<&bool> {
        match self {
            Value::Boolean(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_bool(self) -> Result<bool, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for i64 {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Integer(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_int(&self) -> bool {
        matches!(self, Value::Integer(_))
    }

    #[inline]
    pub fn as_int(&self) -> Option<&i64> {
        match self {
            Value::Integer(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_int(self) -> Result<i64, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for f64 {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    #[inline]
    pub fn as_float(&self) -> Option<&f64> {
        match self {
            Value::Float(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_float(self) -> Result<f64, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bytes(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self, Value::Bytes(_))
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_bytes(self) -> Result<Vec<u8>, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for String {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    #[inline]
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_string(self) -> Result<String, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for Vec<Value> {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::List(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    #[inline]
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_list(self) -> Result<Vec<Value>, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for HashMap<String, Value> {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Map(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }

    #[inline]
    pub fn as_map(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Map(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_map(self) -> Result<HashMap<String, Value>, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for spatial::Cartesian2D {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Cartesian2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_cartesian_2d(&self) -> bool {
        matches!(self, Value::Cartesian2D(_))
    }

    #[inline]
    pub fn as_cartesian_2d(&self) -> Option<&spatial::Cartesian2D> {
        match self {
            Value::Cartesian2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_cartesian_2d(self) -> Result<spatial::Cartesian2D, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for spatial::Cartesian3D {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Cartesian3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_cartesian_3d(&self) -> bool {
        matches!(self, Value::Cartesian3D(_))
    }

    #[inline]
    pub fn as_cartesian_3d(&self) -> Option<&spatial::Cartesian3D> {
        match self {
            Value::Cartesian3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_cartesian_3d(self) -> Result<spatial::Cartesian3D, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for spatial::WGS84_2D {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::WGS84_2D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_wgs84_2d(&self) -> bool {
        matches!(self, Value::WGS84_2D(_))
    }

    #[inline]
    pub fn as_wgs84_2d(&self) -> Option<&spatial::WGS84_2D> {
        match self {
            Value::WGS84_2D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_wgs84_2d(self) -> Result<spatial::WGS84_2D, Self> {
        self.try_into()
    }
}

impl TryFrom<Value> for spatial::WGS84_3D {
    type Error = Value;

    #[inline]
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::WGS84_3D(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Value {
    #[inline]
    pub fn is_wgs84_3d(&self) -> bool {
        matches!(self, Value::WGS84_3D(_))
    }

    #[inline]
    pub fn as_wgs84_3d(&self) -> Option<&spatial::WGS84_3D> {
        match self {
            Value::WGS84_3D(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn try_into_wgs84_3d(self) -> Result<spatial::WGS84_3D, Self> {
        self.try_into()
    }
}

// #[derive(Debug)]
// #[non_exhaustive]
// pub enum RefValue<'a> {
//     Null,
//     Boolean(bool),
//     Integer(i64),
//     Float(f64),
//     Bytes(&'a [u8]),
//     String(&'a str),
//     RefList(&'a [RefValue<'a>]),
//     List(&'a [Value]),
//     RefMap(&'a HashMap<&'a str, RefValue<'a>>),
//     Map(&'a HashMap<String, Value>),
//     Cartesian2D(spatial::Cartesian2D),
//     Cartesian3D(spatial::Cartesian3D),
//     WGS84_2D(spatial::WGS84_2D),
//     WGS84_3D(spatial::WGS84_3D),
//     BrokenValue { reason: &'a str },
// }

macro_rules! impl_value_from_into {
    ( $value:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for Value {
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
            impl From<$ty> for Value {
                fn from(value: $ty) -> Self {
                    $value(value)
                }
            }
        )*
    };
}

impl_value_from_into!(Value::Boolean, bool);
impl_value_from_into!(Value::Integer, u8, u16, u32, i8, i16, i32, i64);
impl_value_from_into!(Value::Float, f32, f64);
impl_value_from_into!(Value::String, &str);

impl_value_from_owned!(Value::String, String);
// impl_value_from_owned!(Value::List, Vec<Value>);
// impl_value_from_owned!(Value::Map, HashMap<String, Value>);
impl_value_from_owned!(Value::Cartesian2D, spatial::Cartesian2D);
impl_value_from_owned!(Value::Cartesian3D, spatial::Cartesian3D);
impl_value_from_owned!(Value::WGS84_2D, spatial::WGS84_2D);
impl_value_from_owned!(Value::WGS84_3D, spatial::WGS84_3D);

impl<T: Into<Value>> From<HashMap<String, T>> for Value {
    fn from(value: HashMap<String, T>) -> Self {
        Value::Map(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(value: Vec<T>) -> Self {
        Value::List(value.into_iter().map(|v| v.into()).collect())
    }
}

// macro_rules! impl_ref_value_from_into {
//     ( $value:expr, $($ty:ty),* ) => {
//         $(
//             impl From<$ty> for RefValue<'static> {
//                 fn from(value: $ty) -> Self {
//                     $value(value.into())
//                 }
//             }
//
//             impl From<&$ty> for RefValue<'static> {
//                 fn from(value: &$ty) -> Self {
//                     $value((*value).into())
//                 }
//             }
//         )*
//     };
// }
//
// macro_rules! impl_ref_value_from_ref {
//     ( $value:expr, $($ty:ty),* ) => {
//         $(
//             impl<'a> From<$ty> for RefValue<'a> {
//                 fn from(value: $ty) -> Self {
//                     $value(value)
//                 }
//             }
//         )*
//     };
// }

// impl_ref_value_from_into!(RefValue::Boolean, bool);
// impl_ref_value_from_into!(RefValue::Integer, u8, u16, u32, i8, i16, i32, i64);
// impl_ref_value_from_into!(RefValue::Float, f32, f64);
// impl_ref_value_from_into!(RefValue::Cartesian2D, spatial::Cartesian2D);
// impl_ref_value_from_into!(RefValue::Cartesian3D, spatial::Cartesian3D);
// impl_ref_value_from_into!(RefValue::WGS84_2D, spatial::WGS84_2D);
// impl_ref_value_from_into!(RefValue::WGS84_3D, spatial::WGS84_3D);
//
// impl_ref_value_from_ref!(RefValue::String, &'a str);
// impl_ref_value_from_ref!(RefValue::RefList, &'a [RefValue<'a>]);
// impl_ref_value_from_ref!(RefValue::List, &'a [Value]);
// impl_ref_value_from_ref!(RefValue::RefMap, &'a HashMap<&'a str, RefValue<'a>>);
// impl_ref_value_from_ref!(RefValue::Map, &'a HashMap<String, Value>);
//
// // impl<'a, T: Into<RefValue<'a>>> From<&'a HashMap<&'a str, T>> for RefValue<'a> {
// //     fn from(value: &'a HashMap<&'a str, T>) -> Self {
// //         return RefValue::Map(value.into_iter().map(|(k, v)| (*k, v.into())).collect());
// //     }
// // }
// //
// // impl<'a, T: Into<RefValue<'a>>> From<&'a [T]> for RefValue<'a> {
// //     fn from(value: &'a [T]) -> Self {
// //         RefValue::List(value.into_iter().map(|v| *v.into()).collect())
// //     }
// // }
//
// impl<'a> From<&Value> for RefValue<'a> {
//     fn from(value: &Value) -> Self {
//         match value {
//             Value::Null => RefValue::Null,
//             Value::Boolean(v) => RefValue::Boolean(*v),
//             Value::Integer(v) => RefValue::Integer(*v),
//             Value::Float(v) => RefValue::Float(*v),
//             Value::Bytes(v) => RefValue::Bytes(v),
//             Value::String(v) => RefValue::String(v),
//             Value::List(v) => RefValue::List(v),
//             Value::Map(v) => RefValue::Map(v),
//             Value::Cartesian2D(v) => RefValue::Cartesian2D(*v),
//             Value::Cartesian3D(v) => RefValue::Cartesian3D(*v),
//             Value::WGS84_2D(v) => RefValue::WGS84_2D(*v),
//             Value::WGS84_3D(v) => RefValue::WGS84_3D(*v),
//             Value::BrokenValue { reason } => RefValue::BrokenValue { reason },
//         }
//     }
// }
