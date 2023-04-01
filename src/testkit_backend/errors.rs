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

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use super::responses::Response;
use super::MaybeDynError;

#[derive(Debug, Clone)]
pub(crate) struct TestKitProtocolError {
    pub(crate) message: String,
}

impl TestKitProtocolError {
    pub(crate) fn new<M: Into<String>>(message: M) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(crate) fn err<M: Into<String>>(message: M) -> MaybeDynError {
        Err(Box::new(Self::new(message)))
    }
}

impl Display for TestKitProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestKitProtocolError: {}", self.message)
    }
}

impl Error for TestKitProtocolError {}

impl TestKitWrappableError for TestKitProtocolError {
    fn wrap(&self) -> Response {
        Response::BackendError {
            msg: format!("unexpected TestKit protocol: {self:?}"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct EndOfStream {}

impl Display for EndOfStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "end of stream")
    }
}

impl Error for EndOfStream {}

pub(crate) trait TestKitWrappableError: Error {
    fn wrap(&self) -> Response;
}

impl TestKitWrappableError for serde_json::Error {
    fn wrap(&self) -> Response {
        Response::BackendError {
            msg: format!("unexpected message format: {self:?}"),
        }
    }
}
