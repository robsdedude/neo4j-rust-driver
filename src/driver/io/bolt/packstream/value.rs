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

use super::super::BoltStructTranslator;
use super::{PackStreamDeserialize, PackStreamSerialize, PackStreamSerializer};
use crate::spatial;
use crate::{ValueReceive, ValueSend};

impl PackStreamSerialize for ValueSend {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        (&self).serialize(serializer, bolt)
    }
}

impl PackStreamSerialize for &ValueSend {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        match self {
            ValueSend::Null => serializer.write_null(),
            ValueSend::Boolean(v) => serializer.write_bool(*v),
            ValueSend::Integer(v) => serializer.write_int(*v),
            ValueSend::Float(v) => serializer.write_float(*v),
            ValueSend::Bytes(v) => serializer.write_bytes(v),
            ValueSend::String(v) => serializer.write_string(v),
            ValueSend::List(v) => serializer.write_list(bolt, v),
            ValueSend::Map(v) => serializer.write_dict(bolt, v),
            ValueSend::Cartesian2D(v) => bolt.serialize_point_2d(serializer, 7203, v.x(), v.y()),
            ValueSend::Cartesian3D(v) => {
                bolt.serialize_point_3d(serializer, 9157, v.x(), v.y(), v.z())
            }
            ValueSend::WGS84_2D(v) => {
                bolt.serialize_point_2d(serializer, 4326, v.longitude(), v.latitude())
            }
            ValueSend::WGS84_3D(v) => {
                bolt.serialize_point_3d(serializer, 4979, v.longitude(), v.latitude(), v.altitude())
            }
        }
    }
}

impl PackStreamDeserialize for ValueReceive {
    type Value = ValueReceive;

    fn load_null() -> Self::Value {
        ValueReceive::Null
    }

    fn load_bool(b: bool) -> Self::Value {
        ValueReceive::Boolean(b)
    }

    fn load_int(i: i64) -> Self::Value {
        ValueReceive::Integer(i)
    }

    fn load_float(f: f64) -> Self::Value {
        ValueReceive::Float(f)
    }

    fn load_bytes(b: Vec<u8>) -> Self::Value {
        ValueReceive::Bytes(b)
    }

    fn load_string(s: String) -> Self::Value {
        ValueReceive::String(s)
    }

    fn load_list(l: Vec<Self::Value>) -> Self::Value {
        ValueReceive::List(l)
    }

    fn load_dict(d: HashMap<String, Self::Value>) -> Self::Value {
        ValueReceive::Map(d)
    }

    fn load_point_2d(fields: Vec<Self::Value>) -> Self::Value {
        if let [ValueReceive::Integer(srid), ValueReceive::Float(x), ValueReceive::Float(y)] =
            fields[..]
        {
            return match srid {
                spatial::SRID_CARTESIAN_2D => {
                    ValueReceive::Cartesian2D(spatial::Cartesian2D::new(x, y))
                }
                spatial::SRID_WGS84_2D => ValueReceive::WGS84_2D(spatial::WGS84_2D::new(x, y)),
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
        if let [ValueReceive::Integer(srid), ValueReceive::Float(x), ValueReceive::Float(y), ValueReceive::Float(z)] =
            fields[..]
        {
            return match srid {
                spatial::SRID_CARTESIAN_3D => {
                    ValueReceive::Cartesian3D(spatial::Cartesian3D::new(x, y, z))
                }
                spatial::SRID_WGS84_3D => ValueReceive::WGS84_3D(spatial::WGS84_3D::new(x, y, z)),
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
        ValueReceive::BrokenValue { reason }
    }
}
