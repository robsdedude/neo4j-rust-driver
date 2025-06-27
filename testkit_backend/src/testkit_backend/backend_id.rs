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

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub(super) struct BackendId(u64);

impl Display for BackendId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", String::from(*self), self.0)
    }
}

impl From<BackendId> for String {
    fn from(id: BackendId) -> Self {
        format!("{}", id.0)
    }
}

impl FromStr for BackendId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        u64::from_str(s)
            .map_err(|err| format!("Invalid BackendId: {err}"))
            .map(BackendId)
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

impl Visitor<'_> for BackendIdVisitor {
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

#[derive(Debug)]
struct SimpleIdGenerator {
    next: u64,
}

impl SimpleIdGenerator {
    fn new(start: u64) -> Self {
        Self { next: start }
    }

    fn generate(&mut self) -> u64 {
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        id
    }
}

#[derive(Debug, Clone)]
pub(super) struct Generator {
    generator: Arc<Mutex<SimpleIdGenerator>>,
}

impl Generator {
    pub(super) fn new() -> Self {
        Self {
            generator: Arc::new(Mutex::new(SimpleIdGenerator::new(0))),
        }
    }

    pub(super) fn next_id(&self) -> BackendId {
        let mut generator = self.generator.lock().unwrap();
        BackendId(generator.generate())
    }
}
