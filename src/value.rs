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

mod de;
mod ser;
pub mod spatial;

#[derive(Debug)]
#[non_exhaustive]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Value>),
    Dictionary(HashMap<String, Value>),
    Cartesian2D(spatial::Cartesian2D),
    Cartesian3D(spatial::Cartesian3D),
    WGS84_2D(spatial::WGS84_2D),
    WGS84_3D(spatial::WGS84_3D),
    BrokenValue,
}

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
impl_value_from_owned!(Value::List, Vec<Value>);
impl_value_from_owned!(Value::Dictionary, HashMap<String, Value>);
impl_value_from_owned!(Value::Cartesian2D, spatial::Cartesian2D);
impl_value_from_owned!(Value::Cartesian3D, spatial::Cartesian3D);
impl_value_from_owned!(Value::WGS84_2D, spatial::WGS84_2D);
impl_value_from_owned!(Value::WGS84_3D, spatial::WGS84_3D);
