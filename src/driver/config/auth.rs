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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::ServerError;
use crate::private::Sealed;
use crate::value::value_send::ValueSend;
use crate::value_map;

#[derive(Debug, Clone, PartialEq)]
pub struct AuthToken {
    pub(crate) data: HashMap<String, ValueSend>,
}

impl AuthToken {
    pub fn new_none_auth() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn new_basic_auth(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            data: value_map!({
                "scheme": "basic",
                "principal": username.into(),
                "credentials": password.into(),
            }),
        }
    }

    pub fn new_basic_auth_with_realm(
        username: impl Into<String>,
        password: impl Into<String>,
        realm: impl Into<String>,
    ) -> Self {
        let mut token = Self::new_basic_auth(username, password);
        token.data.insert("realm".into(), realm.into().into());
        token
    }

    pub fn new_kerberos_auth(base64_encoded_ticket: impl Into<String>) -> Self {
        Self {
            data: value_map!({
                "scheme": "kerberos",
                "principal": "",
                "credentials": base64_encoded_ticket.into(),
            }),
        }
    }

    pub fn new_bearer_auth(base64_encoded_token: impl Into<String>) -> Self {
        Self {
            data: value_map!({
                "scheme": "bearer",
                "credentials": base64_encoded_token.into(),
            }),
        }
    }

    pub fn new_custom_auth(
        principal: Option<String>,
        credentials: Option<String>,
        realm: Option<String>,
        scheme: Option<String>,
        parameters: Option<HashMap<String, ValueSend>>,
    ) -> Self {
        let mut data = HashMap::with_capacity(
            <bool as Into<usize>>::into(principal.is_some())
                + <bool as Into<usize>>::into(credentials.is_some())
                + <bool as Into<usize>>::into(realm.is_some())
                + 1
                + <bool as Into<usize>>::into(parameters.is_some()),
        );
        if let Some(principal) = principal {
            data.insert("principal".into(), principal.into());
        }
        if let Some(credentials) = credentials {
            data.insert("credentials".into(), credentials.into());
        }
        if let Some(realm) = realm {
            data.insert("realm".into(), realm.into());
        }
        data.insert("scheme".into(), scheme.into());
        if let Some(parameters) = parameters {
            data.insert("parameters".into(), parameters.into());
        }
        Self { data }
    }

    pub fn eq_data(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            return true;
        }
        if self.data.len() != other.data.len() {
            return false;
        }
        self.data
            .iter()
            .all(|(k1, v2)| other.data.get(k1).map_or(false, |v1| v1.eq_data(v2)))
    }
}

impl Default for AuthToken {
    fn default() -> Self {
        Self::new_none_auth()
    }
}

pub trait AuthManager: Send + Sync + Debug {
    fn get_auth(&self) -> Arc<AuthToken>;
    fn handle_security_exception(&self, auth: &AuthToken, error: &ServerError) -> bool {
        false
    }
}

pub struct AuthManagers {
    private: (),
}

impl AuthManagers {
    pub fn static_manager(auth: AuthToken) -> impl AuthManager {
        StaticAuthManager {
            auth: Arc::new(auth),
        }
    }
}

#[derive(Debug)]
struct StaticAuthManager {
    auth: Arc<AuthToken>,
}

impl AuthManager for StaticAuthManager {
    fn get_auth(&self) -> Arc<AuthToken> {
        Arc::clone(&self.auth)
    }
}
