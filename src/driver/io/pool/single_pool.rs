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
use std::collections::{HashMap, VecDeque};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex};

use itertools::Itertools;

use super::super::bolt::{self, TcpBolt};
use super::PoolConfig;
use crate::{Address, Result, Value};

type PoolElement = TcpBolt;

#[derive(Debug)]
pub(crate) struct InnerPool {
    config: Arc<PoolConfig>,
    synced: Mutex<InnerPoolInMutex>,
    condition: Condvar,
}

#[derive(Debug)]
struct InnerPoolInMutex {
    raw_pool: VecDeque<PoolElement>,
    reservations: usize,
    borrowed: usize,
}

impl InnerPool {
    fn new(config: Arc<PoolConfig>) -> Self {
        let raw_pool = VecDeque::with_capacity(config.max_connection_pool_size);
        let synced = Mutex::new(InnerPoolInMutex {
            raw_pool,
            reservations: 0,
            borrowed: 0,
        });
        Self {
            config,
            synced,
            condition: Condvar::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SinglePool(Arc<InnerPool>);

impl SinglePool {
    pub(crate) fn new(config: Arc<PoolConfig>) -> Self {
        Self(Arc::new(InnerPool::new(config)))
    }

    pub(crate) fn acquire(&self) -> Result<PooledBolt> {
        {
            let mut synced = self.synced.lock().unwrap();
            loop {
                if let Some(connection) = self.acquire_existing(&mut synced) {
                    return Ok(PooledBolt::new(connection, Arc::clone(&self.0)));
                }
                if self.has_room(&synced) {
                    synced.reservations += 1;
                    break;
                } else {
                    synced = self.condition.wait(synced).unwrap();
                }
            }
        }
        let connection = self.acquire_new()?;
        Ok(PooledBolt::new(connection, Arc::clone(&self.0)))
    }

    pub(crate) fn in_use(&self) -> usize {
        let synced = self.synced.lock().unwrap();
        synced.borrowed + synced.reservations
    }

    fn has_room(&self, synced: &InnerPoolInMutex) -> bool {
        synced.raw_pool.len() + synced.borrowed + synced.reservations
            < self.config.max_connection_pool_size
    }

    fn acquire_existing(&self, synced: &mut InnerPoolInMutex) -> Option<PoolElement> {
        let connection = synced.raw_pool.pop_front();
        if connection.is_some() {
            synced.borrowed += 1;
        }
        connection
    }

    fn acquire_new(&self) -> Result<PoolElement> {
        let connection = self.open_new();
        let mut sync = self.synced.lock().unwrap();
        sync.reservations -= 1;
        let connection = connection?;
        sync.borrowed += 1;
        Ok(connection)
    }

    fn open_new(&self) -> Result<PoolElement> {
        let mut connection = bolt::open(&self.config.address)?;
        connection.hello(
            self.config.user_agent.as_str(),
            &self.config.auth,
            self.config.routing_context.as_ref(),
        )?;
        connection.write_all()?;
        connection.read_all()?;
        Ok(connection)
    }

    fn release(inner_pool: &Arc<InnerPool>, mut connection: PoolElement) {
        let mut lock = inner_pool.synced.lock().unwrap();
        lock.borrowed -= 1;
        if connection.needs_reset() {
            let _ = connection
                .reset()
                .and_then(|_| {
                    let res = connection.write_all();
                    res
                })
                .and_then(|_| {
                    let res = connection.read_all();
                    res
                });
        }
        if !connection.closed() {
            lock.raw_pool.push_back(connection);
        }
        inner_pool.condition.notify_one();
    }
}

impl Deref for SinglePool {
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
    bolt: ManuallyDrop<PoolElement>,
}

impl PooledBolt {
    fn new(bolt: PoolElement, pool: Arc<InnerPool>) -> Self {
        Self {
            pool,
            bolt: ManuallyDrop::new(bolt),
        }
    }
}

impl Drop for PooledBolt {
    fn drop(&mut self) {
        // safety: we're not using ManuallyDrop after this call
        let bolt;
        unsafe {
            bolt = ManuallyDrop::take(&mut self.bolt);
        }
        SinglePool::release(&self.pool, bolt);
    }
}

impl Deref for PooledBolt {
    type Target = TcpBolt;

    fn deref(&self) -> &Self::Target {
        &self.bolt
    }
}

impl DerefMut for PooledBolt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bolt
    }
}
