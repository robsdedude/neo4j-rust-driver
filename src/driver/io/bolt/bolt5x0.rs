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

use super::packstream::{PackStreamDeserialize, PackStreamSerializer};
use super::BoltStructTranslator;
use crate::ValueSend;

pub(crate) struct Bolt5x0StructTranslator {}

impl BoltStructTranslator for Bolt5x0StructTranslator {
    fn serialize_point_2d<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        x: f64,
        y: f64,
    ) -> Result<(), S::Error> {
        serializer.write_struct(
            self,
            b'X',
            &[
                ValueSend::Integer(srid),
                ValueSend::Float(x),
                ValueSend::Float(y),
            ],
        )
    }

    fn serialize_point_3d<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        x: f64,
        y: f64,
        z: f64,
    ) -> Result<(), S::Error> {
        serializer.write_struct(
            self,
            b'Y',
            &[
                ValueSend::Integer(srid),
                ValueSend::Float(x),
                ValueSend::Float(y),
                ValueSend::Float(z),
            ],
        )
    }

    fn deserialize_struct<V: PackStreamDeserialize>(
        &self,
        tag: u8,
        fields: Vec<V::Value>,
    ) -> V::Value {
        match tag {
            b'X' => V::load_point_2d(fields),
            b'Y' => V::load_point_3d(fields),
            _ => V::load_broken(format!("Unknown struct tag {:#02X}", tag)),
        }
    }
}
