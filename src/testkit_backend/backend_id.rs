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

use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snowflaked::Generator as SnowflakeGenerator;
use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub(crate) struct BackendId(u64);

impl From<BackendId> for String {
    fn from(id: BackendId) -> Self {
        BASE64_STANDARD_NO_PAD.encode(id.0.to_be_bytes())
    }
}

impl FromStr for BackendId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = match BASE64_STANDARD_NO_PAD.decode(s) {
            Ok(bytes) => bytes,
            Err(err) => return Err(format!("Invalid BackendId: {err}")),
        };
        let bytes: [u8; 8] = match bytes.try_into() {
            Ok(bytes) => bytes,
            Err(bytes) => {
                return Err(format!(
                    "Invalid BackendId: expected 8 bytes received {}",
                    bytes.len()
                ))
            }
        };
        Ok(BackendId(u64::from_be_bytes(bytes)))
    }
}

impl Serialize for BackendId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&String::from(*self))
    }
}

impl<'de> Deserialize<'de> for BackendId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BackendIdVisitor)
    }
}

struct BackendIdVisitor;

impl<'de> Visitor<'de> for BackendIdVisitor {
    type Value = BackendId;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a base64 encoded big endian u64 as BackendId")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match v.parse() {
            Ok(id) => Ok(id),
            Err(err) => Err(E::custom(err)),
        }
    }
}

pub(crate) struct Generator {
    generator: SnowflakeGenerator,
}

impl Generator {
    pub(crate) fn new() -> Self {
        Self {
            generator: SnowflakeGenerator::new(0),
        }
    }

    pub(crate) fn next_id(&mut self) -> BackendId {
        BackendId(self.generator.generate())
    }
}
