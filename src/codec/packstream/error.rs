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

use std::io;

#[derive(thiserror::Error, Debug)]
#[error("{reason}")]
pub struct PackStreamError {
    reason: String,
    #[source]
    cause: Option<io::Error>,
}

impl From<String> for PackStreamError {
    fn from(reason: String) -> Self {
        PackStreamError {
            reason,
            cause: None,
        }
    }
}

impl From<&str> for PackStreamError {
    fn from(reason: &str) -> Self {
        String::from(reason).into()
    }
}

impl From<io::Error> for PackStreamError {
    fn from(err: io::Error) -> Self {
        let mut e: PackStreamError = format!("IO failure: {}", err).into();
        e.cause = Some(err);
        e
    }
}
