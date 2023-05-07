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

use std::collections::VecDeque;
// TODO: should be able to replace ManuallyDrop with Option::take for safe code
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex};

use super::super::bolt::{self, TcpBolt};
use super::PoolConfig;
use crate::{Address, Result};

type PoolElement = TcpBolt;

#[derive(Debug)]
pub(crate) struct InnerPool {
    address: Arc<Address>,
    config: Arc<PoolConfig>,
    synced: Mutex<InnerPoolSyncedData>,
    made_room_condition: Condvar,
}

#[derive(Debug)]
struct InnerPoolSyncedData {
    raw_pool: VecDeque<PoolElement>,
    reservations: usize,
    borrowed: usize,
}

impl InnerPool {
    fn new(address: Arc<Address>, config: Arc<PoolConfig>) -> Self {
        let raw_pool = VecDeque::with_capacity(config.max_connection_pool_size);
        let synced = Mutex::new(InnerPoolSyncedData {
            raw_pool,
            reservations: 0,
            borrowed: 0,
        });
        Self {
            address,
            config,
            synced,
            made_room_condition: Condvar::new(),
        }
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
        let mut connection = bolt::open(Arc::clone(&self.address))?;
        connection.hello(
            self.config.user_agent.as_str(),
            &self.config.auth,
            self.config.routing_context.as_ref(),
        )?;
        connection.write_all()?;
        connection.read_all()?;
        Ok(connection)
    }
}

#[derive(Debug)]
pub(crate) struct SimplePool(Arc<InnerPool>);

impl SimplePool {
    pub(crate) fn new(address: Arc<Address>, config: Arc<PoolConfig>) -> Self {
        Self(Arc::new(InnerPool::new(address, config)))
    }

    pub(crate) fn acquire(&self) -> UnpreparedSinglePooledBolt {
        {
            let mut synced = self.synced.lock().unwrap();
            loop {
                if let Some(connection) = self.acquire_existing(&mut synced) {
                    return UnpreparedSinglePooledBolt::new(Some(connection), Arc::clone(&self.0));
                }
                if self.has_room(&synced) {
                    synced.reservations += 1;
                    break;
                } else {
                    synced = self.made_room_condition.wait(synced).unwrap();
                }
            }
        }
        UnpreparedSinglePooledBolt::new(None, Arc::clone(&self.0))
    }

    pub(crate) fn acquire_no_wait(&self) -> Option<UnpreparedSinglePooledBolt> {
        {
            let mut synced = self.synced.lock().unwrap();
            if let Some(connection) = self.acquire_existing(&mut synced) {
                return Some(UnpreparedSinglePooledBolt::new(
                    Some(connection),
                    Arc::clone(&self.0),
                ));
            }
            if self.has_room(&synced) {
                synced.reservations += 1;
            } else {
                return None;
            }
        }
        Some(UnpreparedSinglePooledBolt::new(None, Arc::clone(&self.0)))
    }

    pub(crate) fn acquire_idle(&self) -> Option<SinglePooledBolt> {
        let mut synced = self.synced.lock().unwrap();
        self.acquire_existing(&mut synced)
            .map(|connection| SinglePooledBolt::new(connection, Arc::clone(&self.0)))
    }

    pub(crate) fn in_use(&self) -> usize {
        let synced = self.synced.lock().unwrap();
        synced.borrowed + synced.reservations
    }

    fn has_room(&self, synced: &InnerPoolSyncedData) -> bool {
        synced.raw_pool.len() + synced.borrowed + synced.reservations
            < self.config.max_connection_pool_size
    }

    fn acquire_existing(&self, synced: &mut InnerPoolSyncedData) -> Option<PoolElement> {
        let connection = synced.raw_pool.pop_front();
        if connection.is_some() {
            synced.borrowed += 1;
        }
        connection
    }

    fn release(inner_pool: &Arc<InnerPool>, mut connection: PoolElement) {
        let mut lock = inner_pool.synced.lock().unwrap();
        lock.borrowed -= 1;
        if connection.needs_reset() {
            let _ = connection
                .reset()
                .and_then(|_| connection.write_all())
                .and_then(|_| connection.read_all());
        }
        if !connection.closed() {
            lock.raw_pool.push_back(connection);
        }
        inner_pool.made_room_condition.notify_one();
    }
}

impl Deref for SimplePool {
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
pub(crate) struct UnpreparedSinglePooledBolt {
    pool: Arc<InnerPool>,
    bolt: Option<ManuallyDrop<PoolElement>>,
}

impl UnpreparedSinglePooledBolt {
    fn new(bolt: Option<PoolElement>, pool: Arc<InnerPool>) -> Self {
        Self {
            pool,
            bolt: bolt.map(ManuallyDrop::new),
        }
    }

    pub(crate) fn prepare(mut self) -> Result<Option<SinglePooledBolt>> {
        let bolt = self.bolt.take();
        let pool = Arc::clone(&self.pool);
        match bolt {
            None => {
                let connection = self.pool.acquire_new()?;
                Ok(Some(SinglePooledBolt::new(connection, pool)))
            }
            Some(bolt) => {
                // room for health check etc. (return None of failed health check)
                Ok(Some(SinglePooledBolt { pool, bolt }))
            }
        }
    }
}

impl Drop for UnpreparedSinglePooledBolt {
    fn drop(&mut self) {
        let Some(drop_bolt) = &mut self.bolt else {
            return;
        };
        // safety: we're not using ManuallyDrop after this call
        let bolt;
        unsafe {
            bolt = ManuallyDrop::take(drop_bolt);
        }
        SimplePool::release(&self.pool, bolt);
    }
}

#[derive(Debug)]
pub(crate) struct SinglePooledBolt {
    pool: Arc<InnerPool>,
    bolt: ManuallyDrop<PoolElement>,
}

impl SinglePooledBolt {
    fn new(bolt: PoolElement, pool: Arc<InnerPool>) -> Self {
        Self {
            pool,
            bolt: ManuallyDrop::new(bolt),
        }
    }
}

impl Drop for SinglePooledBolt {
    fn drop(&mut self) {
        // safety: we're not using ManuallyDrop after this call
        let bolt;
        unsafe {
            bolt = ManuallyDrop::take(&mut self.bolt);
        }
        SimplePool::release(&self.pool, bolt);
    }
}

impl Deref for SinglePooledBolt {
    type Target = TcpBolt;

    fn deref(&self) -> &Self::Target {
        &self.bolt
    }
}

impl DerefMut for SinglePooledBolt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bolt
    }
}
