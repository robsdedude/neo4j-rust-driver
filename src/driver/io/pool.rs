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

use atomic_refcell::AtomicRefCell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex};

use itertools::Itertools;

use super::bolt::{self, TcpBolt};
use crate::{Address, Result, Value};

#[derive(Debug)]
pub(crate) struct PoolConfig {
    pub(crate) address: Address,
    // pub(crate) routing: bool,
    pub(crate) routing_context: Option<HashMap<String, Value>>,
    // pub(crate) ssl_context: Option<SslContext>,
    pub(crate) user_agent: String,
    pub(crate) auth: HashMap<String, Value>,
    pub(crate) max_connection_pool_size: usize,
}

type PoolConnection = Arc<AtomicRefCell<TcpBolt>>;

#[derive(Debug)]
pub(crate) struct InnerPool {
    config: PoolConfig,
    synced: Mutex<InnerPoolInMutex>,
    condition: Condvar,
}

#[derive(Debug)]
struct InnerPoolInMutex {
    raw_pool: Vec<PoolConnection>,
    reservations: usize,
}

impl InnerPool {
    fn new(config: PoolConfig) -> Self {
        let raw_pool = Vec::with_capacity(config.max_connection_pool_size);
        let synced = Mutex::new(InnerPoolInMutex {
            raw_pool,
            reservations: 0,
        });
        Self {
            config,
            synced,
            condition: Condvar::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Pool(Arc<InnerPool>);

impl Pool {
    pub(crate) fn new(config: PoolConfig) -> Self {
        Self(Arc::new(InnerPool::new(config)))
    }

    pub(crate) fn acquire(&self) -> Result<PooledBolt> {
        {
            let mut synced = self.synced.lock().unwrap();
            loop {
                if let Some(connection) = self.acquire_existing(&mut synced.raw_pool) {
                    return Ok(PooledBolt::new(
                        Arc::clone(&connection),
                        Arc::clone(&self.0),
                    ));
                }
                if self.has_room(&synced) {
                    synced.reservations += 1;
                    break;
                } else {
                    synced = self.condition.wait(synced).unwrap();
                }
            }
        }
        let connection = self.open_new();
        let mut sync = self.synced.lock().unwrap();
        sync.reservations -= 1;
        let connection = connection?;
        sync.raw_pool.push(Arc::clone(&connection));
        Ok(PooledBolt::new(connection, Arc::clone(&self.0)))
    }

    fn has_room(&self, synced: &InnerPoolInMutex) -> bool {
        synced.raw_pool.len() + synced.reservations < self.config.max_connection_pool_size
    }

    fn acquire_existing(&self, connections: &mut Vec<PoolConnection>) -> Option<PoolConnection> {
        for connection in connections {
            if Arc::strong_count(connection) == 1 {
                return Some(Arc::clone(connection));
            }
        }
        None
    }

    fn open_new(&self) -> Result<PoolConnection> {
        let mut connection = bolt::open(&self.config.address)?;
        connection.hello(
            self.config.user_agent.as_str(),
            &self.config.auth,
            self.config.routing_context.as_ref(),
        )?;
        connection.write_all()?;
        connection.read_all()?;
        Ok(Arc::new(AtomicRefCell::new(connection)))
    }

    /// safety: drops the ManuallyDrop: don't use it afterwards!
    unsafe fn release(inner_pool: &Arc<InnerPool>, connection: &mut ManuallyDrop<PoolConnection>) {
        let mut lock = inner_pool.synced.lock().unwrap();
        if connection.deref().borrow().closed() {
            if let Some((idx, _)) = lock
                .raw_pool
                .iter()
                .find_position(|c| Arc::ptr_eq(c, connection))
            {
                lock.raw_pool.remove(idx);
            }
        }
        unsafe {
            ManuallyDrop::drop(connection);
        }
        inner_pool.condition.notify_one();
    }
}

impl Deref for Pool {
    type Target = InnerPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// while TcpBolt is not Send, the pool asserts that only ever one thread
// is mutating each connection
// unsafe impl Send for Pool {}
// unsafe impl Sync for Pool {}

#[derive(Debug)]
pub(crate) struct PooledBolt {
    pool: Arc<InnerPool>,
    bolt: ManuallyDrop<PoolConnection>,
}

impl PooledBolt {
    fn new(bolt: PoolConnection, pool: Arc<InnerPool>) -> Self {
        Self {
            pool,
            bolt: ManuallyDrop::new(bolt),
        }
    }
}

impl Drop for PooledBolt {
    fn drop(&mut self) {
        // safety: we're not using bolt after this call
        unsafe {
            Pool::release(&self.pool, &mut self.bolt);
        }
    }
}

impl Deref for PooledBolt {
    type Target = AtomicRefCell<TcpBolt>;

    fn deref(&self) -> &Self::Target {
        self.bolt.deref()
    }
}
