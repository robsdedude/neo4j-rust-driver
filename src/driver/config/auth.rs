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

use parking_lot::Mutex;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Instant;

use crate::error::{ServerError, UserCallbackError};
use crate::value::value_send::ValueSend;
use crate::value_map;
use crate::{Neo4jError, Result};

type BoxError = Box<dyn StdError + Send + Sync>;
pub type ManagerGetAuthReturn = StdResult<Arc<AuthToken>, BoxError>;
pub type ManagerHandleErrReturn = StdResult<bool, BoxError>;
pub type BasicProviderReturn = StdResult<AuthToken, BoxError>;
pub type BearerProviderReturn = StdResult<(AuthToken, Option<Instant>), BoxError>;

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
        #[inline]
        fn used(arg: &Option<String>) -> bool {
            arg.as_ref().map(|s| !s.is_empty()).unwrap_or(false)
        }

        let mut data = HashMap::with_capacity(
            <bool as Into<usize>>::into(principal.is_some())
                + <bool as Into<usize>>::into(used(&credentials))
                + <bool as Into<usize>>::into(used(&realm))
                + <bool as Into<usize>>::into(used(&scheme))
                + 1
                + <bool as Into<usize>>::into(
                    parameters.as_ref().map(|s| !s.is_empty()).unwrap_or(false),
                ),
        );
        if let Some(principal) = principal {
            data.insert("principal".into(), principal.into());
        }
        if let Some(credentials) = credentials {
            if !credentials.is_empty() {
                data.insert("credentials".into(), credentials.into());
            }
        }
        if let Some(realm) = realm {
            if !realm.is_empty() {
                data.insert("realm".into(), realm.into());
            }
        }
        data.insert("scheme".into(), scheme.into());
        if let Some(parameters) = parameters {
            if !parameters.is_empty() {
                data.insert("parameters".into(), parameters.into());
            }
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

    #[inline]
    pub fn data(&self) -> &HashMap<String, ValueSend> {
        &self.data
    }
}

impl Default for AuthToken {
    fn default() -> Self {
        Self::new_none_auth()
    }
}

pub trait AuthManager: Send + Sync + Debug {
    fn get_auth(&self) -> ManagerGetAuthReturn;
    fn handle_security_error(
        &self,
        _auth: &Arc<AuthToken>,
        _error: &ServerError,
    ) -> ManagerHandleErrReturn {
        Ok(false)
    }
}

pub struct AuthManagers {
    private: (),
}

impl AuthManagers {
    pub fn new_static(auth: AuthToken) -> impl AuthManager {
        StaticAuthManager {
            auth: Arc::new(auth),
        }
    }

    pub fn new_basic<P: Fn() -> BasicProviderReturn + Sync + Send>(
        provider: P,
    ) -> impl AuthManager {
        BasicAuthManager(Neo4jAuthManager {
            provider,
            handled_codes: ["Neo.ClientError.Security.Unauthorized"],
            cached_auth: Default::default(),
        })
    }

    pub fn new_bearer<P: Fn() -> BearerProviderReturn + Send + Sync>(
        provider: P,
    ) -> impl AuthManager {
        BearerAuthManager(Neo4jAuthManager {
            provider,
            handled_codes: [
                "Neo.ClientError.Security.TokenExpired",
                "Neo.ClientError.Security.Unauthorized",
            ],
            cached_auth: Default::default(),
        })
    }

    pub(crate) fn get_auth(manager: &'_ dyn AuthManager) -> Result<Arc<AuthToken>> {
        manager.get_auth().map_err(|err| Neo4jError::UserCallback {
            error: UserCallbackError::AuthManagerError(err),
        })
    }

    pub(crate) fn handle_security_error(
        manager: &'_ dyn AuthManager,
        auth: &Arc<AuthToken>,
        error: &ServerError,
    ) -> Result<bool> {
        manager
            .handle_security_error(auth, error)
            .map_err(|err| Neo4jError::UserCallback {
                error: UserCallbackError::AuthManagerError(err),
            })
    }
}

#[derive(Debug)]
struct StaticAuthManager {
    auth: Arc<AuthToken>,
}

impl AuthManager for StaticAuthManager {
    fn get_auth(&self) -> ManagerGetAuthReturn {
        Ok(Arc::clone(&self.auth))
    }
}

#[derive(Debug)]
struct Neo4jAuthCache {
    auth: Arc<AuthToken>,
    expiry: Option<Instant>,
}

struct Neo4jAuthManager<P, const N: usize> {
    provider: P,
    handled_codes: [&'static str; N],
    cached_auth: Mutex<Option<Neo4jAuthCache>>,
}

impl<P, const N: usize> Neo4jAuthManager<P, N> {
    fn handle_security_error(
        &self,
        auth: &AuthToken,
        error: &ServerError,
    ) -> ManagerHandleErrReturn {
        if !self.handled_codes.contains(&error.code()) {
            return Ok(false);
        }
        let mut cache_guard = self.cached_auth.lock();
        let Some(cached_auth) = &*cache_guard else {
            return Ok(true);
        };
        if auth.eq_data(&cached_auth.auth) {
            *cache_guard = None;
        }
        Ok(true)
    }
}

struct BasicAuthManager<P, const N: usize>(Neo4jAuthManager<P, N>);
impl<P, const N: usize> Debug for BasicAuthManager<P, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jBasicAuthManager")
            .field("handled_codes", &self.0.handled_codes)
            .field("cached_auth", &self.0.cached_auth)
            .finish()
    }
}

struct BearerAuthManager<P, const N: usize>(Neo4jAuthManager<P, N>);
impl<P, const N: usize> Debug for BearerAuthManager<P, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jBearerAuthManager")
            .field("handled_codes", &self.0.handled_codes)
            .field("cached_auth", &self.0.cached_auth)
            .finish()
    }
}

impl<P: Fn() -> BasicProviderReturn + Sync + Send, const N: usize> AuthManager
    for BasicAuthManager<P, N>
{
    fn get_auth(&self) -> ManagerGetAuthReturn {
        let mut cache_guard = self.0.cached_auth.lock();
        if let Some(cache) = &*cache_guard {
            return Ok(Arc::clone(&cache.auth));
        }
        let auth = Arc::new((self.0.provider)()?);
        *cache_guard = Some(Neo4jAuthCache {
            auth: Arc::clone(&auth),
            expiry: None,
        });
        Ok(auth)
    }

    #[inline]
    fn handle_security_error(
        &self,
        auth: &Arc<AuthToken>,
        error: &ServerError,
    ) -> ManagerHandleErrReturn {
        self.0.handle_security_error(auth, error)
    }
}

impl<P: Fn() -> BearerProviderReturn + Sync + Send, const N: usize> AuthManager
    for BearerAuthManager<P, N>
{
    fn get_auth(&self) -> ManagerGetAuthReturn {
        let mut cache_guard = self.0.cached_auth.lock();
        if let Some(cache) = &*cache_guard {
            let expired = match cache.expiry {
                Some(expiry) => expiry <= Instant::now(),
                None => false,
            };
            if !expired {
                return Ok(Arc::clone(&cache.auth));
            }
            *cache_guard = None;
        }
        let (auth, expiry) = (self.0.provider)()?;
        let auth = Arc::new(auth);
        *cache_guard = Some(Neo4jAuthCache {
            auth: Arc::clone(&auth),
            expiry,
        });
        Ok(auth)
    }

    #[inline]
    fn handle_security_error(
        &self,
        auth: &Arc<AuthToken>,
        error: &ServerError,
    ) -> ManagerHandleErrReturn {
        self.0.handle_security_error(auth, error)
    }
}
