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

use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use super::io::PooledBolt;
use crate::driver::io::bolt::ResponseCallbacks;
use crate::driver::RecordStream;
use crate::value::PackStreamSerialize;
use crate::{Result, ValueReceive};

#[derive(Debug)]
pub struct Transaction<'driver: 'tx, 'tx> {
    inner: &'tx mut InnerTransaction<'driver>,
}

impl<'driver: 'tx, 'tx> Transaction<'driver, 'tx> {
    pub(crate) fn new(inner: &'tx mut InnerTransaction<'driver>) -> Self {
        Self { inner }
    }

    pub fn run<Q: AsRef<str>>(&self, query: Q) -> Result<RecordStream<'driver>> {
        let cx = Rc::clone(&self.inner.connection);

        let run_prep = cx
            .borrow_mut()
            .run_prepare(query.as_ref(), None, None, None, None)?;
        let mut record_stream = RecordStream::new(cx, false);
        record_stream.run(run_prep)?;
        Ok(record_stream)
    }

    pub fn run_with_parameters<
        Q: AsRef<str>,
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
    >(
        &self,
        query: Q,
        parameters: P,
    ) -> Result<RecordStream<'driver>> {
        let cx = Rc::clone(&self.inner.connection);

        let mut run_prep = cx
            .borrow_mut()
            .run_prepare(query.as_ref(), None, None, None, None)?;
        if !parameters.borrow().is_empty() {
            run_prep.with_parameters(parameters.borrow())?;
        }
        let mut record_stream = RecordStream::new(cx, false);
        record_stream.run(run_prep)?;
        Ok(record_stream)
    }

    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }

    pub fn rollback(self) -> Result<()> {
        self.inner.rollback()
    }
}

#[derive(Debug)]
pub struct InnerTransaction<'driver> {
    connection: Rc<RefCell<PooledBolt<'driver>>>,
    bookmark: Arc<AtomicRefCell<Option<String>>>,
    closed: bool,
}

impl<'driver> InnerTransaction<'driver> {
    pub(crate) fn new(connection: PooledBolt<'driver>) -> Self {
        Self {
            connection: Rc::new(RefCell::new(connection)),
            bookmark: Default::default(),
            closed: false,
        }
    }

    pub(crate) fn begin<K: AsRef<str> + Debug, S: PackStreamSerialize>(
        &mut self,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_metadata: &HashMap<K, S>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<()> {
        let mut cx = self.connection.borrow_mut();
        let tx_metadata = if tx_metadata.is_empty() {
            None
        } else {
            Some(tx_metadata)
        };
        cx.begin(bookmarks, tx_timeout, tx_metadata, mode, db, imp_user)?;
        cx.write_all()?;
        cx.read_all()
    }

    pub fn commit(&mut self) -> Result<()> {
        self.closed = true;
        let mut cx = self.connection.borrow_mut();
        let bookmark = Arc::clone(&self.bookmark);
        cx.commit(ResponseCallbacks::new().with_on_success(move |mut meta| {
            if let Some(ValueReceive::String(bms)) = meta.remove("bookmark") {
                *bookmark.borrow_mut() = Some(bms);
            };
            Ok(())
        }))?;
        cx.write_all()?;
        cx.read_all()
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.closed = true;
        let mut cx = self.connection.borrow_mut();
        cx.rollback()?;
        cx.write_all()?;
        cx.read_all()
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.closed {
            return self.rollback();
        }
        Ok(())
    }

    pub(crate) fn into_bookmark(mut self) -> Option<String> {
        self.bookmark.borrow_mut().take()
    }
}
