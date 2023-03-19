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

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::sleep;
use std::time::Duration;

use crate::Result;

#[derive(Debug)]
pub(crate) struct MostlyRLock<T: Debug> {
    inner: RwLock<T>,
    updating: AtomicBool,
}

impl<T: Debug> MostlyRLock<T> {
    pub(crate) fn new(inner: T) -> Self {
        let inner = RwLock::new(inner);
        Self {
            inner,
            updating: AtomicBool::new(false),
        }
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<T> {
        self.inner.read().unwrap()
    }

    pub(crate) fn update<'a, UPDATE: FnMut(RwLockWriteGuard<'a, T>) -> Result<()>>(
        &'a self,
        mut updater: UPDATE,
    ) -> Result<RwLockReadGuard<'a, T>> {
        let mut done = false;
        self.maybe_write(
            |_| {
                let res = !done;
                done = !done;
                res
            },
            |lock| updater(lock),
        )
    }

    pub(crate) fn maybe_write<
        'a,
        CHECK: FnMut(&RwLockReadGuard<'a, T>) -> bool,
        UPDATE: FnMut(RwLockWriteGuard<'a, T>) -> Result<()>,
    >(
        &'a self,
        mut needs_update: CHECK,
        mut updater: UPDATE,
    ) -> Result<RwLockReadGuard<'a, T>> {
        loop {
            {
                let r_lock = self.inner.read().unwrap();
                if !needs_update(&r_lock) {
                    return Ok(r_lock);
                }
            }
            let updating = self.updating.swap(true, Ordering::SeqCst);
            if updating {
                let w_lock = self.inner.write().unwrap();
                updater(w_lock)?;
                self.updating.store(false, Ordering::SeqCst);
                return Ok(self.inner.read().unwrap());
            } else {
                // avoid drowning the writer
                sleep(Duration::from_millis(1))
            }
        }
    }

    /*    pub(crate) fn maybe_update<
            'a,
            CHECK: FnMut(&RwLockReadGuard<'a, T>) -> bool,
            UPDATE: FnMut(RwLockWriteGuard<'a, T>) -> Result<()>,
        >(
            &'a self,
            needs_update: CHECK,
            updater: UPDATE,
        ) -> Result<()> {
            drop(self.maybe_write(needs_update, updater)?);
            Ok(())
        }
    */
}
