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

use std::io::Read;

use super::packstream::{PackStreamDeserialize, PackStreamDeserializerImpl};
use crate::{Neo4jError, Result};

#[derive(Debug)]
pub(crate) struct BoltMessage<V: PackStreamDeserialize> {
    pub tag: u8,
    pub fields: Vec<V::Value>,
}

impl<V: PackStreamDeserialize> BoltMessage<V> {
    pub(crate) fn load<R, CB>(reader: &mut R, mut load_value: CB) -> Result<Self>
    where
        R: Read,
        CB: FnMut(&mut R) -> Result<V::Value>,
    {
        let mut marker = [0; 1];
        reader.read_exact(&mut marker)?;
        let marker = marker[0];
        if !(0xB0..=0xBF).contains(&marker) {
            return Err(Neo4jError::ProtocolError {
                message: format!("expected bolt message marker, received {:02X?}", marker),
            });
        }
        let size = marker - 0xB0;
        let mut tag = [0; 1];
        reader.read_exact(&mut tag)?;
        let tag = u8::from_be_bytes(tag);
        let mut deserializer = PackStreamDeserializerImpl::new(reader);
        let fields = (0..size)
            .map(|_| load_value(reader))
            .collect::<Result<_>>()?;
        Ok(BoltMessage { tag, fields })
    }
}
