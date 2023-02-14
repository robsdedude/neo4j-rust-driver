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

use super::super::BoltStructTranslator;
use super::{PackStreamDeserialize, PackStreamSerialize, PackStreamSerializer};
use crate::value::spatial;
use crate::Value;
use std::collections::HashMap;

impl PackStreamSerialize for Value {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        (&self).serialize(serializer, bolt)
    }
}

impl PackStreamSerialize for &Value {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        match self {
            Value::Null => serializer.write_null(),
            Value::Boolean(v) => serializer.write_bool(*v),
            Value::Integer(v) => serializer.write_int(*v),
            Value::Float(v) => serializer.write_float(*v),
            Value::Bytes(v) => serializer.write_bytes(v),
            Value::String(v) => serializer.write_string(v),
            Value::List(v) => serializer.write_list(bolt, v),
            Value::Map(v) => serializer.write_dict(bolt, v),
            Value::Cartesian2D(v) => bolt.serialize_point_2d(serializer, 7203, v.x(), v.y()),
            Value::Cartesian3D(v) => bolt.serialize_point_3d(serializer, 9157, v.x(), v.y(), v.z()),
            Value::WGS84_2D(v) => {
                bolt.serialize_point_2d(serializer, 4326, v.longitude(), v.latitude())
            }
            Value::WGS84_3D(v) => {
                bolt.serialize_point_3d(serializer, 4979, v.longitude(), v.latitude(), v.height())
            }
            v @ Value::BrokenValue { .. } => serializer.error(format!("cannot send `{:?}`", v)),
        }
    }
}

// impl PackStreamSerialize for &HashMap<&str, &Value> {
//     fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
//         &self,
//         serializer: &mut S,
//         bolt: &B,
//     ) -> Result<(), S::Error> {
//         serializer.write_dict(bolt, self)
//     }
// }
//
// impl PackStreamSerialize for &[&Value] {
//     fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
//         &self,
//         serializer: &mut S,
//         bolt: &B,
//     ) -> Result<(), S::Error> {
//         serializer.write_list(bolt, self)
//     }
// }

// impl PackStreamSerialize for RefValue<'_> {
//     fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
//         &self,
//         serializer: &mut S,
//         bolt: &B,
//     ) -> Result<(), S::Error> {
//         match self {
//             RefValue::Null => serializer.write_null(),
//             RefValue::Boolean(v) => serializer.write_bool(*v),
//             RefValue::Integer(v) => serializer.write_int(*v),
//             RefValue::Float(v) => serializer.write_float(*v),
//             RefValue::Bytes(v) => serializer.write_bytes(v),
//             RefValue::String(v) => serializer.write_string(v),
//             RefValue::RefList(v) => serializer.write_list(bolt, v),
//             RefValue::List(v) => serializer.write_list(bolt, v),
//             RefValue::RefMap(v) => serializer.write_dict(bolt, v),
//             RefValue::Map(v) => serializer.write_dict(bolt, v),
//             RefValue::Cartesian2D(v) => bolt.serialize_point_2d(serializer, v.srid(), v.x(), v.y()),
//             RefValue::Cartesian3D(v) => {
//                 bolt.serialize_point_3d(serializer, v.srid(), v.x(), v.y(), v.z())
//             }
//             RefValue::WGS84_2D(v) => {
//                 bolt.serialize_point_2d(serializer, v.srid(), v.longitude(), v.latitude())
//             }
//             RefValue::WGS84_3D(v) => bolt.serialize_point_3d(
//                 serializer,
//                 v.srid(),
//                 v.longitude(),
//                 v.latitude(),
//                 v.height(),
//             ),
//             v @ RefValue::BrokenValue { .. } => serializer.error(format!("cannot send `{:?}`", v)),
//         }
//     }
// }

impl PackStreamDeserialize for Value {
    type Value = Value;

    fn load_null() -> Self::Value {
        Value::Null
    }

    fn load_bool(b: bool) -> Self::Value {
        Value::Boolean(b)
    }

    fn load_int(i: i64) -> Self::Value {
        Value::Integer(i)
    }

    fn load_float(f: f64) -> Self::Value {
        Value::Float(f)
    }

    fn load_bytes(b: Vec<u8>) -> Self::Value {
        Value::Bytes(b)
    }

    fn load_string(s: String) -> Self::Value {
        Value::String(s)
    }

    fn load_list(l: Vec<Self::Value>) -> Self::Value {
        Value::List(l)
    }

    fn load_dict(d: HashMap<String, Self::Value>) -> Self::Value {
        Value::Map(d)
    }

    fn load_point_2d(fields: Vec<Self::Value>) -> Self::Value {
        if let [Value::Integer(srid), Value::Float(x), Value::Float(y)] = fields[..] {
            return match srid {
                spatial::SRID_CARTESIAN_2D => Value::Cartesian2D(spatial::Cartesian2D::new(x, y)),
                spatial::SRID_WGS84_2D => Value::WGS84_2D(spatial::WGS84_2D::new(x, y)),
                _ => Self::load_broken(format!("Unknown 2D SRID {}", srid)),
            };
        }
        Self::load_broken(format!(
            "2D point requires 3 fields (SRID: i64, x: f64, y: f64), found {} ({:?})",
            fields.len(),
            fields
        ))
    }

    fn load_point_3d(fields: Vec<Self::Value>) -> Self::Value {
        if let [Value::Integer(srid), Value::Float(x), Value::Float(y), Value::Float(z)] =
            fields[..]
        {
            return match srid {
                spatial::SRID_CARTESIAN_3D => {
                    Value::Cartesian3D(spatial::Cartesian3D::new(x, y, z))
                }
                spatial::SRID_WGS84_3D => Value::WGS84_3D(spatial::WGS84_3D::new(x, y, z)),
                _ => Self::load_broken(format!("Unknown 3D SRID {}", srid)),
            };
        }
        Self::load_broken(format!(
            "3D point requires 4 fields (SRID: i64, x: f64, y: f64, z: f64), found {} ({:?})",
            fields.len(),
            fields
        ))
    }

    fn load_broken(reason: String) -> Self::Value {
        Value::BrokenValue { reason }
    }
}
