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

// TODO: remove when prototyping phase is done
#![allow(dead_code)]

extern crate core;

mod address;
mod driver;
mod error;
mod util;
mod value;

pub use address::Address;
pub use driver::{
    ConnectionConfig, Driver, DriverConfig, PackStreamDeserialize, PackStreamSerialize, Record,
    RecordStream, Session, SessionConfig, SessionRunConfig, Summary,
};
pub use error::{Neo4jError, Result};
pub use value::Value;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
