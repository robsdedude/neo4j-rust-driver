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

use std::mem::size_of;

use usize_cast::FromUsize;

use super::super::bolt5x8::Bolt5x8StructTranslator;
use super::super::bolt_common::*;
use super::super::{BoltStructTranslator, PackStreamSerializer};
use crate::value::{vector, ValueReceive, ValueSend};

#[derive(Debug)]
pub(crate) struct Bolt6x0StructTranslator {
    utc_patch: bool,
    bolt5x8_translator: Bolt5x8StructTranslator,
}

impl BoltStructTranslator for Bolt6x0StructTranslator {
    fn new(bolt_version: ServerAwareBoltVersion) -> Self {
        Self {
            utc_patch: false,
            bolt5x8_translator: Bolt5x8StructTranslator::new(bolt_version),
        }
    }

    fn serialize<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        value: &ValueSend,
    ) -> Result<(), S::Error> {
        if self.utc_patch {
            return self.bolt5x8_translator.serialize(serializer, value);
        }
        match value {
            ValueSend::Vector(vector::Vector::F64(v)) => {
                serializer.write_struct_header(TAG_VECTOR, 2)?;
                serializer.write_bytes(VEC_TYPE_MARKER_F64)?;
                serializer.write_bytes_header(u64::from_usize(size_of::<f64>() * v.len()))?;
                v.iter()
                    .try_for_each(|v| serializer.write_bytes_raw(&v.to_be_bytes()))
            }
            ValueSend::Vector(vector::Vector::F32(v)) => {
                serializer.write_struct_header(TAG_VECTOR, 2)?;
                serializer.write_bytes(VEC_TYPE_MARKER_F32)?;
                serializer.write_bytes_header(u64::from_usize(size_of::<f32>() * v.len()))?;
                v.iter()
                    .try_for_each(|v| serializer.write_bytes_raw(&v.to_be_bytes()))
            }
            ValueSend::Vector(vector::Vector::I64(v)) => {
                serializer.write_struct_header(TAG_VECTOR, 2)?;
                serializer.write_bytes(VEC_TYPE_MARKER_I64)?;
                serializer.write_bytes_header(u64::from_usize(size_of::<i64>() * v.len()))?;
                v.iter()
                    .try_for_each(|v| serializer.write_bytes_raw(&v.to_be_bytes()))
            }
            ValueSend::Vector(vector::Vector::I32(v)) => {
                serializer.write_struct_header(TAG_VECTOR, 2)?;
                serializer.write_bytes(VEC_TYPE_MARKER_I32)?;
                serializer.write_bytes_header(u64::from_usize(size_of::<i32>() * v.len()))?;
                v.iter()
                    .try_for_each(|v| serializer.write_bytes_raw(&v.to_be_bytes()))
            }
            ValueSend::Vector(vector::Vector::I16(v)) => {
                serializer.write_struct_header(TAG_VECTOR, 2)?;
                serializer.write_bytes(VEC_TYPE_MARKER_I16)?;
                serializer.write_bytes_header(u64::from_usize(size_of::<i16>() * v.len()))?;
                v.iter()
                    .try_for_each(|v| serializer.write_bytes_raw(&v.to_be_bytes()))
            }
            ValueSend::Vector(vector::Vector::I8(v)) => {
                serializer.write_struct_header(TAG_VECTOR, 2)?;
                serializer.write_bytes(VEC_TYPE_MARKER_I8)?;
                serializer.write_bytes_header(u64::from_usize(size_of::<i8>() * v.len()))?;
                v.iter()
                    .try_for_each(|v| serializer.write_bytes_raw(&v.to_be_bytes()))
            }
            _ => self.bolt5x8_translator.serialize(serializer, value),
        }
    }

    fn deserialize_struct(&self, tag: u8, mut fields: Vec<ValueReceive>) -> ValueReceive {
        match tag {
            TAG_VECTOR => {
                let size = fields.len();
                if size != 2 {
                    return invalid_struct(format!(
                        "expected 2 fields for vector struct b'V', found {size}"
                    ));
                }
                let data = as_bytes!(fields.pop().unwrap(), "vector data");
                let type_marker = as_bytes!(fields.pop().unwrap(), "vector type marker");
                ValueReceive::Vector(match type_marker.as_slice() {
                    VEC_TYPE_MARKER_F64 => {
                        if data.len() % size_of::<f64>() != 0 {
                            return invalid_struct(format!(
                                "f64 vector data misaligned (not multiple of {} bytes)",
                                size_of::<f64>()
                            ));
                        }
                        let data = data
                            .chunks_exact(size_of::<f64>())
                            .map(|f| f64::from_be_bytes(f.try_into().unwrap()))
                            .collect();
                        vector::Vector::F64(data)
                    }
                    VEC_TYPE_MARKER_F32 => {
                        if data.len() % size_of::<f32>() != 0 {
                            return invalid_struct(format!(
                                "f32 vector data misaligned (not multiple of {} bytes)",
                                size_of::<f32>()
                            ));
                        }
                        let data = data
                            .chunks_exact(size_of::<f32>())
                            .map(|f| f32::from_be_bytes(f.try_into().unwrap()))
                            .collect();
                        vector::Vector::F32(data)
                    }
                    VEC_TYPE_MARKER_I64 => {
                        if data.len() % size_of::<i64>() != 0 {
                            return invalid_struct(format!(
                                "i64 vector data misaligned (not multiple of {} bytes)",
                                size_of::<i64>()
                            ));
                        }
                        let data = data
                            .chunks_exact(size_of::<i64>())
                            .map(|f| i64::from_be_bytes(f.try_into().unwrap()))
                            .collect();
                        vector::Vector::I64(data)
                    }
                    VEC_TYPE_MARKER_I32 => {
                        if data.len() % size_of::<i32>() != 0 {
                            return invalid_struct(format!(
                                "i32 vector data misaligned (not multiple of {} bytes)",
                                size_of::<i32>()
                            ));
                        }
                        let data = data
                            .chunks_exact(size_of::<i32>())
                            .map(|f| i32::from_be_bytes(f.try_into().unwrap()))
                            .collect();
                        vector::Vector::I32(data)
                    }
                    VEC_TYPE_MARKER_I16 => {
                        if data.len() % size_of::<i16>() != 0 {
                            return invalid_struct(format!(
                                "i16 vector data misaligned (not multiple of {} bytes)",
                                size_of::<i16>()
                            ));
                        }
                        let data = data
                            .chunks_exact(size_of::<i16>())
                            .map(|f| i16::from_be_bytes(f.try_into().unwrap()))
                            .collect();
                        vector::Vector::I16(data)
                    }
                    VEC_TYPE_MARKER_I8 => {
                        let data = data
                            .chunks(1)
                            .map(|f| i8::from_be_bytes(f.try_into().unwrap()))
                            .collect();
                        vector::Vector::I8(data)
                    }
                    _ => {
                        return invalid_struct(format!(
                            "unknown vector type marker {type_marker:?}"
                        ));
                    }
                })
            }
            _ => self.bolt5x8_translator.deserialize_struct(tag, fields),
        }
    }
}
