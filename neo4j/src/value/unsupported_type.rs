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

/// Represents a type unknown to the driver, received from the server.
///
/// This type is used, for instance, when a newer DBMS produces a result containing a type that the
/// current version of the driver does not yet understand.
///
/// The attributes exposed by this type are meant for displaying and debugging purposes.
/// They may change in future versions of the server and should not be relied upon for any logic in
/// your application.
/// If your application requires handling this type, you must upgrade your driver to a version that
/// supports it.
#[derive(Debug, Clone)]
pub struct UnsupportedType {
    pub(crate) name: String,
    pub(crate) minimum_protocol_version: (u8, u8),
    pub(crate) message: Option<String>,
}

impl UnsupportedType {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn minimum_protocol_version(&self) -> (u8, u8) {
        self.minimum_protocol_version
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

impl PartialEq for UnsupportedType {
    fn eq(&self, _: &Self) -> bool {
        false
    }
}
