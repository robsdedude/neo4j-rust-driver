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

use crate::driver::session::Bookmarks;
use crate::driver::RoutingControl;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub(crate) database: Option<String>,
    pub(crate) bookmarks: Option<Vec<String>>,
    pub(crate) impersonated_user: Option<String>,
    pub(crate) run_routing: RoutingControl,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            database: None,
            bookmarks: None,
            impersonated_user: None,
            run_routing: RoutingControl::Write,
        }
    }
}

impl SessionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    pub fn with_default_database(mut self) -> Self {
        self.database = None;
        self
    }

    pub fn with_bookmarks(mut self, bookmarks: Bookmarks) -> Self {
        self.bookmarks = Some(bookmarks.into_raw().collect());
        self
    }

    pub fn without_bookmarks(mut self) -> Self {
        self.bookmarks = None;
        self
    }

    pub fn with_impersonated_user(mut self, user: String) -> Self {
        self.impersonated_user = Some(user);
        self
    }

    pub fn without_impersonated_user(mut self) -> Self {
        self.impersonated_user = None;
        self
    }

    pub fn run_routing(mut self, mode: RoutingControl) -> Self {
        self.run_routing = mode;
        self
    }
}

impl AsRef<SessionConfig> for SessionConfig {
    #[inline]
    fn as_ref(&self) -> &SessionConfig {
        self
    }
}
