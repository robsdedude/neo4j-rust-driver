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

use thiserror::Error;

use crate::driver::session::Bookmarks;

const DEFAULT_FETCH_SIZE: i64 = 1000;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub(crate) database: Option<String>,
    pub(crate) bookmarks: Option<Vec<String>>,
    pub(crate) impersonated_user: Option<String>,
    pub(crate) fetch_size: i64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            database: None,
            bookmarks: None,
            impersonated_user: None,
            fetch_size: DEFAULT_FETCH_SIZE,
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

    /// fetch_size must be <= i64::MAX
    pub fn with_fetch_size(
        mut self,
        fetch_size: u64,
    ) -> Result<Self, ConfigureFetchSizeError<Self>> {
        match i64::try_from(fetch_size) {
            Ok(fetch_size) => {
                self.fetch_size = fetch_size;
                Ok(self)
            }
            Err(_) => Err(ConfigureFetchSizeError { builder: self }),
        }
    }

    pub fn with_fetch_all(mut self) -> Self {
        self.fetch_size = -1;
        self
    }

    pub fn with_default_fetch_size(mut self) -> Self {
        self.fetch_size = DEFAULT_FETCH_SIZE;
        self
    }
}

impl AsRef<SessionConfig> for SessionConfig {
    #[inline]
    fn as_ref(&self) -> &SessionConfig {
        self
    }
}

#[derive(Debug, Error)]
#[error("fetch size must be <= i64::MAX")]
pub struct ConfigureFetchSizeError<Builder> {
    pub builder: Builder,
}
