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
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use super::io::bolt::ResponseCallbacks;
use super::io::PooledBolt;
use super::record_stream::RecordStream;
use super::record_stream::SharedErrorPropagator;
use crate::error::ServerError;
use crate::summary::Summary;
use crate::value::PackStreamSerialize;
use crate::{Neo4jError, Result, ValueReceive};

#[derive(Debug)]
pub struct Transaction<'driver: 'tx, 'tx> {
    inner: &'tx mut InnerTransaction<'driver>,
    drop_result: RefCell<Result<()>>,
}

/// > NOTE:
/// > Once any associated function of the transaction or any `TransactionRecordStream`
/// > spawned from it returns an error, the transaction is closed.
impl<'driver: 'inner_tx, 'inner_tx> Transaction<'driver, 'inner_tx> {
    pub(crate) fn new(inner: &'inner_tx mut InnerTransaction<'driver>) -> Self {
        Self {
            inner,
            drop_result: RefCell::new(Ok(())),
        }
    }

    pub fn run<'tx, Q: AsRef<str>>(
        &'tx self,
        query: Q,
    ) -> Result<TransactionRecordStream<'driver, 'tx, 'inner_tx>> {
        Ok(TransactionRecordStream(self.inner.run(query)?, &self))
    }

    pub fn run_with_parameters<
        'tx,
        Q: AsRef<str>,
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
    >(
        &'tx self,
        query: Q,
        parameters: P,
    ) -> Result<TransactionRecordStream<'driver, 'tx, 'inner_tx>> {
        Ok(TransactionRecordStream(
            self.inner.run_with_parameters(query, parameters)?,
            &self,
        ))
    }

    pub fn commit(self) -> Result<()> {
        self.drop_result.into_inner()?;
        self.inner.commit()
    }

    pub fn rollback(self) -> Result<()> {
        self.drop_result.into_inner()?;
        self.inner.rollback()
    }
}

#[derive(Debug)]
pub struct TransactionRecordStream<'driver, 'tx, 'inner_tx>(
    RecordStream<'driver>,
    &'tx Transaction<'driver, 'inner_tx>,
);

impl<'driver, 'tx, 'inner_tx> Deref for TransactionRecordStream<'driver, 'tx, 'inner_tx> {
    type Target = RecordStream<'driver>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'driver, 'tx, 'inner_tx> DerefMut for TransactionRecordStream<'driver, 'tx, 'inner_tx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'driver, 'tx, 'inner_tx> Drop for TransactionRecordStream<'driver, 'tx, 'inner_tx> {
    fn drop(&mut self) {
        if let Err(err) = self.0.consume() {
            if self.1.drop_result.borrow().is_ok() {
                let _ = self.1.drop_result.replace(Err(err));
            }
        }
    }
}

impl<'driver, 'tx, 'inner_tx> TransactionRecordStream<'driver, 'tx, 'inner_tx> {
    /// see `RecordStream::consume` (except that this consumes `self`)
    pub fn consume(mut self) -> Result<Option<Summary>> {
        self.0.consume()
    }
}

#[derive(Debug)]
pub(crate) struct InnerTransaction<'driver> {
    connection: Rc<RefCell<PooledBolt<'driver>>>,
    bookmark: Arc<AtomicRefCell<Option<String>>>,
    error_propagator: SharedErrorPropagator,
    fetch_size: i64,
    closed: bool,
}

impl<'driver> InnerTransaction<'driver> {
    pub(crate) fn new(connection: PooledBolt<'driver>, fetch_size: i64) -> Self {
        Self {
            connection: Rc::new(RefCell::new(connection)),
            bookmark: Default::default(),
            error_propagator: Default::default(),
            fetch_size,
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

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.closed = true;
        self.check_error()?;
        let mut cx = self.connection.borrow_mut();
        let bookmark = Arc::clone(&self.bookmark);
        cx.write_all()?;
        cx.read_all()?;
        cx.commit(
            ResponseCallbacks::new()
                .with_on_success(move |mut meta| {
                    if let Some(ValueReceive::String(bms)) = meta.remove("bookmark") {
                        *bookmark.borrow_mut() = Some(bms);
                    };
                    Ok(())
                })
                .with_on_failure(|meta| Err(ServerError::from_meta(meta).into())),
        )?;
        cx.write_all()?;
        Neo4jError::wrap_commit(cx.read_all())
    }

    pub(crate) fn rollback(&mut self) -> Result<()> {
        self.closed = true;
        self.check_error()?;
        let mut cx = self.connection.borrow_mut();
        cx.rollback()?;
        cx.write_all()?;
        cx.read_all()
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        if self.check_error().is_err() || self.connection.borrow_mut().closed() {
            self.closed = true;
        }
        if !self.closed {
            return self.rollback();
        }
        Ok(())
    }

    pub(crate) fn into_bookmark(self) -> Option<String> {
        self.bookmark.borrow_mut().take()
    }

    pub(crate) fn run<Q: AsRef<str>>(&self, query: Q) -> Result<RecordStream<'driver>> {
        let cx = Rc::clone(&self.connection);

        let run_prep = cx
            .borrow_mut()
            .run_prepare(query.as_ref(), None, None, None, None)?;
        let mut record_stream = RecordStream::new(
            cx,
            self.fetch_size,
            false,
            Some(Arc::clone(&self.error_propagator)),
        );
        record_stream.run(run_prep)?;
        Ok(record_stream)
    }

    pub(crate) fn run_with_parameters<
        Q: AsRef<str>,
        K: AsRef<str> + Debug,
        S: PackStreamSerialize,
        P: Borrow<HashMap<K, S>>,
    >(
        &self,
        query: Q,
        parameters: P,
    ) -> Result<RecordStream<'driver>> {
        let cx = Rc::clone(&self.connection);

        let mut run_prep = cx
            .borrow_mut()
            .run_prepare(query.as_ref(), None, None, None, None)?;
        if !parameters.borrow().is_empty() {
            run_prep.with_parameters(parameters.borrow())?;
        }
        let mut record_stream = RecordStream::new(
            cx,
            self.fetch_size,
            false,
            Some(Arc::clone(&self.error_propagator)),
        );
        record_stream.run(run_prep)?;
        Ok(record_stream)
    }

    fn check_error(&self) -> Result<()> {
        match self.error_propagator.deref().borrow().error() {
            None => Ok(()),
            Some(err) => {
                Err(ServerError::new(String::from(err.code()), String::from(err.message())).into())
            }
        }
    }
}
