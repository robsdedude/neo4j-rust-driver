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

use serde::Deserialize;

use super::responses::Response;
use super::{Backend, MaybeDynError};

#[derive(Deserialize, Debug)]
#[serde(tag = "name", content = "data")]
pub(crate) enum Request {
    GetFeatures {},
    #[serde(rename_all = "camelCase")]
    StartTest {
        test_name: String,
    },
}

impl Request {
    pub(crate) fn handle(self, backend: &mut Backend) -> MaybeDynError {
        match self {
            Request::GetFeatures {} => Response::feature_list().send(backend)?,
            Request::StartTest { .. } => Response::RunTest.send(backend)?,
        }
        Ok(())
    }
}
