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

use log::info;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::lock_api::MutexGuard;
use parking_lot::{Condvar, Mutex, RawMutex};

use super::super::bolt::{self, TcpBolt};
use super::PoolConfig;
use crate::address::resolve_address_fully;
use crate::{Address, Neo4jError, Result};

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

    fn acquire_new(&self, deadline: Option<Instant>) -> Result<PoolElement> {
        let connection = self.open_new(deadline);
        let mut sync = self.synced.lock();
        sync.reservations -= 1;
        let connection = connection?;
        sync.borrowed += 1;
        Ok(connection)
    }

    fn open_new(&self, deadline: Option<Instant>) -> Result<PoolElement> {
        let address = Arc::clone(&self.address);
        let mut connection = self.open_socket(address, deadline)?;

        connection.hello(
            self.config.user_agent.as_str(),
            &self.config.auth,
            self.config.routing_context.as_ref(),
        )?;
        connection.write_all(deadline)?;
        connection.read_all(deadline, None)?;
        Ok(connection)
    }

    fn open_socket(&self, address: Arc<Address>, deadline: Option<Instant>) -> Result<TcpBolt> {
        let mut last_err = None;
        for address in resolve_address_fully(address, self.config.resolver.as_deref())? {
            last_err = match address {
                Ok(address) => {
                    match bolt::open(address, deadline, self.config.connection_timeout) {
                        Ok(connection) => return Ok(connection),
                        Err(err) => {
                            info!("failed to open connection: {}", err);
                            Some(Err(err))
                        }
                    }
                }
                Err(err) => {
                    info!("failed to resolve address: {}", err);
                    Some(Err(Neo4jError::connect_error(err)))
                }
            }
        }
        last_err.expect("resolve_address_fully returned empty iterator")
    }
}

#[derive(Debug)]
pub(crate) struct SimplePool(Arc<InnerPool>);

impl SimplePool {
    pub(crate) fn new(address: Arc<Address>, config: Arc<PoolConfig>) -> Self {
        Self(Arc::new(InnerPool::new(address, config)))
    }

    pub(crate) fn acquire(&self, deadline: Option<Instant>) -> Result<UnpreparedSinglePooledBolt> {
        {
            let mut synced = self.synced.lock();
            loop {
                if let Some(connection) = self.acquire_existing(&mut synced) {
                    return Ok(UnpreparedSinglePooledBolt::new(
                        Some(connection),
                        Arc::clone(&self.0),
                    ));
                }
                if self.has_room(&synced) {
                    synced.reservations += 1;
                    break;
                } else {
                    self.wait_for_room(deadline, &mut synced)?;
                }
            }
        }
        Ok(UnpreparedSinglePooledBolt::new(None, Arc::clone(&self.0)))
    }

    fn wait_for_room(
        &self,
        deadline: Option<Instant>,
        synced: &mut MutexGuard<RawMutex, InnerPoolSyncedData>,
    ) -> Result<()> {
        match deadline {
            None => self.made_room_condition.wait(synced),
            Some(deadline) => {
                if self
                    .made_room_condition
                    .wait_until(synced, deadline)
                    .timed_out()
                {
                    return Err(Neo4jError::connection_acquisition_timeout(
                        "waiting for room in the connection pool",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn acquire_no_wait(&self) -> Option<UnpreparedSinglePooledBolt> {
        {
            let mut synced = self.synced.lock();
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
        let mut synced = self.synced.lock();
        self.acquire_existing(&mut synced)
            .map(|connection| SinglePooledBolt::new(connection, Arc::clone(&self.0)))
    }

    pub(crate) fn in_use(&self) -> usize {
        let synced = self.synced.lock();
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
        let mut lock = inner_pool.synced.lock();
        lock.borrowed -= 1;
        if connection.needs_reset() {
            let res = connection
                .reset()
                .and_then(|_| connection.write_all(None))
                .and_then(|_| connection.read_all(None, None));
            if res.is_err() {
                info!("ignoring failure during reset, dropping connection");
            }
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

#[derive(Debug)]
pub(crate) struct UnpreparedSinglePooledBolt {
    pool: Arc<InnerPool>,
    bolt: Option<PoolElement>,
}

impl UnpreparedSinglePooledBolt {
    fn new(bolt: Option<PoolElement>, pool: Arc<InnerPool>) -> Self {
        Self { pool, bolt }
    }

    pub(crate) fn prepare(mut self, deadline: Option<Instant>) -> Result<Option<SinglePooledBolt>> {
        let bolt = self.bolt.take();
        let pool = Arc::clone(&self.pool);
        match bolt {
            None => {
                let connection = self.pool.acquire_new(deadline)?;
                Ok(Some(SinglePooledBolt::new(connection, pool)))
            }
            Some(_) => {
                // room for health check etc. (return None on failed health check)
                Ok(Some(SinglePooledBolt { pool, bolt }))
            }
        }
    }
}

impl Drop for UnpreparedSinglePooledBolt {
    fn drop(&mut self) {
        if self.bolt.is_none() {
            return;
        }
        let bolt = self.bolt.take().unwrap();
        SimplePool::release(&self.pool, bolt);
    }
}

#[derive(Debug)]
pub(crate) struct SinglePooledBolt {
    pool: Arc<InnerPool>,
    bolt: Option<PoolElement>,
}

impl SinglePooledBolt {
    fn new(bolt: PoolElement, pool: Arc<InnerPool>) -> Self {
        Self {
            pool,
            bolt: Some(bolt),
        }
    }
}

impl Drop for SinglePooledBolt {
    fn drop(&mut self) {
        let bolt = self
            .bolt
            .take()
            .expect("bolt option should be Some from init to drop");
        SimplePool::release(&self.pool, bolt);
    }
}

impl Deref for SinglePooledBolt {
    type Target = TcpBolt;

    fn deref(&self) -> &Self::Target {
        self.bolt
            .as_ref()
            .expect("bolt option should be Some from init to drop")
    }
}

impl DerefMut for SinglePooledBolt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bolt
            .as_mut()
            .expect("bolt option should be Some from init to drop")
    }
}
