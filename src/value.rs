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
