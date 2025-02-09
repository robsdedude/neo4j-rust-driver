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

use itertools::Itertools;
use log::debug;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;
use std::time::Instant;

use super::auth::AuthToken;
use crate::value::spatial;
use crate::{value, ValueSend};

#[derive(Debug)]
pub(super) struct HomeDbCache {
    cache: Mutex<HashMap<HomeDbCacheKey, HomeDbCacheEntry>>,
    config: HomeDbCacheConfig,
}

#[derive(Debug, Copy, Clone)]
struct HomeDbCacheConfig {
    max_size: usize,
    prune_size: usize,
}

impl Default for HomeDbCache {
    fn default() -> Self {
        Self::new(1000)
    }
}

impl HomeDbCache {
    pub(super) fn new(max_size: usize) -> Self {
        let max_size_f64 = max_size as f64;
        let prune_size = usize::min(max_size, (max_size_f64 * 0.01).log(max_size_f64) as usize);
        HomeDbCache {
            cache: Mutex::new(HashMap::with_capacity(max_size)),
            config: HomeDbCacheConfig {
                max_size,
                prune_size,
            },
        }
    }

    pub(super) fn get(&self, key: &HomeDbCacheKey) -> Option<Arc<String>> {
        let mut lock = self.cache.lock();
        let cache: &mut HashMap<HomeDbCacheKey, HomeDbCacheEntry> = &mut lock;
        let res = cache.get_mut(key).map(|entry| {
            entry.last_used = Instant::now();
            Arc::clone(&entry.database)
        });
        debug!(
            "Getting home database cache for key: {} -> {:?}",
            key.log_str(),
            res.as_deref(),
        );
        res
    }

    pub(super) fn update(&self, key: HomeDbCacheKey, database: Arc<String>) {
        let mut lock = self.cache.lock();
        debug!(
            "Updating home database cache for key: {} -> {:?}",
            key.log_str(),
            database.as_str(),
        );
        let cache: &mut HashMap<HomeDbCacheKey, HomeDbCacheEntry> = &mut lock;
        let previous_val = cache.insert(
            key,
            HomeDbCacheEntry {
                database,
                last_used: Instant::now(),
            },
        );
        if previous_val.is_none() {
            // cache grew, prune if necessary
            Self::prune(cache, self.config);
        }
    }

    fn prune(cache: &mut HashMap<HomeDbCacheKey, HomeDbCacheEntry>, config: HomeDbCacheConfig) {
        if cache.len() <= config.max_size {
            return;
        }
        debug!(
            "Pruning home database cache to size: {}",
            config.max_size - config.prune_size
        );
        let new_cache = mem::take(cache);
        *cache = new_cache
            .into_iter()
            .sorted_by(|(_, v1), (_, v2)| v2.last_used.cmp(&v1.last_used))
            .take(config.max_size - config.prune_size)
            .collect();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum HomeDbCacheKey {
    DriverUser,
    FixedUser(Arc<String>),
    SessionAuth(SessionAuthKey),
}

impl HomeDbCacheKey {
    fn log_str(&self) -> String {
        match self {
            HomeDbCacheKey::DriverUser | HomeDbCacheKey::FixedUser(_) => format!("{:?}", self),
            HomeDbCacheKey::SessionAuth(SessionAuthKey(auth)) => {
                let mut auth: AuthToken = (**auth).clone();
                auth.data
                    .get_mut("credentials")
                    .map(|c| *c = value!("**********"));
                format!("SessionAuth({:?})", auth.data)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SessionAuthKey(Arc<AuthToken>);

impl PartialEq for SessionAuthKey {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
            || self.0.data.len() == other.0.data.len()
                && self
                    .0
                    .data
                    .iter()
                    .sorted_by(|(k1, _), (k2, _)| k1.cmp(k2))
                    .zip(other.0.data.iter().sorted_by(|(k1, _), (k2, _)| k1.cmp(k2)))
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && v1.eq_data(v2))
    }
}

impl Eq for SessionAuthKey {}

impl Hash for SessionAuthKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0
            .data
            .iter()
            .sorted_by(|(k1, _), (k2, _)| k1.cmp(k2))
            .for_each(|(k, v)| {
                k.hash(state);
                Self::hash(v, state);
            });
    }
}

impl SessionAuthKey {
    fn hash(v: &ValueSend, state: &mut impl Hasher) {
        match v {
            ValueSend::Null => state.write_usize(0),
            ValueSend::Boolean(v) => v.hash(state),
            ValueSend::Integer(v) => v.hash(state),
            ValueSend::Float(v) => v.to_bits().hash(state),
            ValueSend::Bytes(v) => v.hash(state),
            ValueSend::String(v) => v.hash(state),
            ValueSend::List(v) => v.iter().for_each(|v| Self::hash(v, state)),
            ValueSend::Map(v) => {
                v.iter()
                    .sorted_by(|(k1, _), (k2, _)| k1.cmp(k2))
                    .for_each(|(k, v)| {
                        k.hash(state);
                        Self::hash(v, state);
                    });
            }
            ValueSend::Cartesian2D(spatial::Cartesian2D { srid, coordinates }) => {
                srid.hash(state);
                coordinates
                    .iter()
                    .map(|v| v.to_bits())
                    .for_each(|v| v.hash(state));
            }
            ValueSend::Cartesian3D(spatial::Cartesian3D { srid, coordinates }) => {
                srid.hash(state);
                coordinates
                    .iter()
                    .map(|v| v.to_bits())
                    .for_each(|v| v.hash(state));
            }
            ValueSend::WGS84_2D(spatial::WGS84_2D { srid, coordinates }) => {
                srid.hash(state);
                coordinates
                    .iter()
                    .map(|v| v.to_bits())
                    .for_each(|v| v.hash(state));
            }
            ValueSend::WGS84_3D(spatial::WGS84_3D { srid, coordinates }) => {
                srid.hash(state);
                coordinates
                    .iter()
                    .map(|v| v.to_bits())
                    .for_each(|v| v.hash(state));
            }
            ValueSend::Duration(v) => v.hash(state),
            ValueSend::LocalTime(v) => v.hash(state),
            ValueSend::Time(v) => v.hash(state),
            ValueSend::Date(v) => v.hash(state),
            ValueSend::LocalDateTime(v) => v.hash(state),
            ValueSend::DateTime(v) => v.hash(state),
            ValueSend::DateTimeFixed(v) => v.hash(state),
        }
    }
}

impl HomeDbCacheKey {
    pub(super) fn new(
        imp_user: Option<&Arc<String>>,
        session_auth: Option<&Arc<AuthToken>>,
    ) -> Self {
        if let Some(user) = imp_user {
            HomeDbCacheKey::FixedUser(Arc::clone(user))
        } else if let Some(auth) = session_auth {
            if let Some(ValueSend::String(scheme)) = auth.data.get("scheme") {
                if scheme == "basic" {
                    if let Some(ValueSend::String(user)) = auth.data.get("principal") {
                        return HomeDbCacheKey::FixedUser(Arc::new(user.clone()));
                    }
                }
            }
            HomeDbCacheKey::SessionAuth(SessionAuthKey(Arc::clone(auth)))
        } else {
            HomeDbCacheKey::DriverUser
        }
    }
}

#[derive(Debug, Clone)]
struct HomeDbCacheEntry {
    database: Arc<String>,
    last_used: Instant,
}
