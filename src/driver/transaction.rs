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
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::rc::Rc;
use std::result;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use super::io::bolt::message_parameters::{BeginParameters, RunParameters};
use super::io::bolt::ResponseCallbacks;
use super::io::PooledBolt;
use super::record_stream::{GetSingleRecordError, RecordStream, SharedErrorPropagator};
use super::Record;
use crate::bookmarks::Bookmarks;
use crate::error::ServerError;
use crate::summary::Summary;
use crate::{Neo4jError, Result, ValueReceive, ValueSend};

#[derive(Debug)]
pub struct Transaction<'driver: 'inner_tx, 'inner_tx> {
    inner_tx: &'inner_tx mut InnerTransaction<'driver>,
    drop_result: RefCell<Result<()>>,
}

/// > NOTE:
/// > Once any associated function of the transaction or any `TransactionRecordStream`
/// > spawned from it returns an error, the transaction is closed.
impl<'driver: 'inner_tx, 'inner_tx> Transaction<'driver, 'inner_tx> {
    pub(crate) fn new(inner: &'inner_tx mut InnerTransaction<'driver>) -> Self {
        Self {
            inner_tx: inner,
            drop_result: RefCell::new(Ok(())),
        }
    }

    pub fn query<'tx, Q: AsRef<str>>(
        &'tx self,
        query: Q,
    ) -> TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, DefaultKey, DefaultParameters> {
        TransactionQueryBuilder::new(self, query)
    }

    fn run<'tx, Q: AsRef<str>, K: Borrow<str> + Debug, M: Borrow<HashMap<K, ValueSend>>>(
        &'tx self,
        builder: TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, K, M>,
    ) -> Result<TransactionRecordStream<'driver, 'tx, 'inner_tx>> {
        let query = builder.query.as_ref();
        let parameters = builder.parameters.borrow();
        Ok(TransactionRecordStream(
            self.inner_tx.run(query, parameters)?,
            self,
        ))
    }

    pub fn commit(self) -> Result<()> {
        self.drop_result.into_inner()?;
        self.inner_tx.commit()
    }

    pub fn rollback(self) -> Result<()> {
        self.drop_result.into_inner()?;
        self.inner_tx.rollback()
    }
}

#[derive(Debug)]
pub struct TransactionRecordStream<'driver, 'tx, 'inner_tx>(
    RecordStream<'driver>,
    &'tx Transaction<'driver, 'inner_tx>,
);

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
    /// see `RecordStream::keys`
    pub fn keys(&self) -> Vec<Arc<String>> {
        self.0.keys()
    }
    /// see `RecordStream::single`
    pub fn single(&mut self) -> result::Result<Result<Record>, GetSingleRecordError> {
        self.0.single()
    }

    pub(crate) fn raw_stream_mut(&mut self) -> &mut RecordStream<'driver> {
        &mut self.0
    }
}

impl<'driver, 'tx, 'inner_tx> Iterator for TransactionRecordStream<'driver, 'tx, 'inner_tx> {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
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

    pub(crate) fn begin<K: Borrow<str> + Debug>(
        &mut self,
        bookmarks: Option<&Bookmarks>,
        tx_timeout: Option<i64>,
        tx_metadata: &HashMap<K, ValueSend>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
        eager: bool,
    ) -> Result<()> {
        let mut cx = self.connection.borrow_mut();
        let tx_metadata = if tx_metadata.is_empty() {
            None
        } else {
            Some(tx_metadata)
        };
        cx.begin(BeginParameters::new(
            bookmarks,
            tx_timeout,
            tx_metadata,
            mode,
            db,
            imp_user,
        ))?;
        if eager {
            cx.write_all(None)?;
            cx.read_all(None)?;
        }
        Ok(())
    }

    pub(crate) fn commit(&mut self) -> Result<()> {
        self.closed = true;
        self.check_error()?;
        let mut cx = self.connection.borrow_mut();
        let bookmark = Arc::clone(&self.bookmark);
        cx.write_all(None)?;
        cx.read_all(None)?;
        cx.commit(ResponseCallbacks::new().with_on_success(move |mut meta| {
            if let Some(ValueReceive::String(bms)) = meta.remove("bookmark") {
                *bookmark.borrow_mut() = Some(bms);
            };
            Ok(())
        }))?;
        cx.write_all(None)?;
        Neo4jError::wrap_commit(cx.read_all(None))
    }

    pub(crate) fn rollback(&mut self) -> Result<()> {
        self.closed = true;
        if self.error_propagator.deref().borrow().error().is_some() {
            // transaction already failed, nothing to rollback
            return Ok(());
        }
        let mut cx = self.connection.borrow_mut();
        cx.rollback()?;
        cx.write_all(None)?;
        cx.read_all(None)
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

    pub(crate) fn run<K: Borrow<str> + Debug>(
        &self,
        query: &str,
        parameters: &HashMap<K, ValueSend>,
    ) -> Result<RecordStream<'driver>> {
        let cx = Rc::clone(&self.connection);

        let mut record_stream = RecordStream::new(
            cx,
            self.fetch_size,
            false,
            Some(Arc::clone(&self.error_propagator)),
        );
        record_stream.run(RunParameters::new_transaction_run(query, Some(parameters)))?;
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

pub struct TransactionQueryBuilder<
    'driver,
    'tx,
    'inner_tx,
    Q: AsRef<str>,
    K: Borrow<str> + Debug,
    M: Borrow<HashMap<K, ValueSend>>,
> {
    tx: &'tx Transaction<'driver, 'inner_tx>,
    query: Q,
    _k: PhantomData<K>,
    parameters: M,
}

type DefaultKey = String;
type DefaultParameters = HashMap<DefaultKey, ValueSend>;

impl<'driver, 'tx, 'inner_tx, Q: AsRef<str>>
    TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, DefaultKey, DefaultParameters>
{
    fn new(tx: &'tx Transaction<'driver, 'inner_tx>, query: Q) -> Self {
        Self {
            tx,
            query,
            _k: PhantomData,
            parameters: Default::default(),
        }
    }
}

impl<
        'driver,
        'tx,
        'inner_tx,
        Q: AsRef<str>,
        K: Borrow<str> + Debug,
        M: Borrow<HashMap<K, ValueSend>>,
    > TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, K, M>
{
    pub fn with_parameters<K_: Borrow<str> + Debug, M_: Borrow<HashMap<K_, ValueSend>>>(
        self,
        parameters: M_,
    ) -> TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, K_, M_> {
        let Self {
            tx,
            query,
            _k: _,
            parameters: _,
        } = self;
        TransactionQueryBuilder {
            tx,
            query,
            _k: PhantomData,
            parameters,
        }
    }

    pub fn without_parameters(
        self,
    ) -> TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, DefaultKey, DefaultParameters> {
        let Self {
            tx,
            query,
            _k: _,
            parameters: _,
        } = self;
        TransactionQueryBuilder {
            tx,
            query,
            _k: PhantomData,
            parameters: Default::default(),
        }
    }

    pub fn run(self) -> Result<TransactionRecordStream<'driver, 'tx, 'tx>> {
        self.tx.run(self)
    }
}

impl<
        'driver,
        'tx,
        'inner_tx,
        Q: AsRef<str>,
        K: Borrow<str> + Debug,
        M: Borrow<HashMap<K, ValueSend>>,
    > Debug for TransactionQueryBuilder<'driver, 'tx, 'inner_tx, Q, K, M>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionQueryBuilder")
            .field("inner_tx", &self.tx)
            .field("query", &self.query.as_ref())
            .field("parameters", self.parameters.borrow())
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionTimeout {
    timeout: InternalTransactionTimeout,
}

/// Controls after how long a transaction should be killed by the server.
///
/// Choices:
///  * [`TransactionTimeout::none`] never time out
///  * [`TransactionTimeout::from_millis`] time out after specified duration
///  * [`TransactionTimeout::default`] use the default timeout configured on the server.
impl TransactionTimeout {
    /// Construct a transaction timeout in milliseconds.
    ///
    /// The specified timeout overrides the default timeout configured on the server using the
    /// `db.transaction.timeout` setting (`dbms.transaction.timeout` before Neo4j 5.0).
    /// values higher than `db.transaction.timeout` will be ignored and will fall back to the
    /// default for server versions between 4.2 and 5.2 (inclusive).
    ///
    /// This method returns `None` if the timeout is less than or equal to 0 as this is not
    /// considered a valid timeout by the server.
    ///
    /// # Examples
    /// ```
    /// use neo4j::transaction::TransactionTimeout;
    ///
    /// assert!(TransactionTimeout::from_millis(-1).is_none());
    /// assert!(TransactionTimeout::from_millis(0).is_none());
    /// assert!(TransactionTimeout::from_millis(1).is_some());
    /// ```
    #[inline]
    pub fn from_millis(timeout: i64) -> Option<Self> {
        if timeout <= 0 {
            return None;
        }
        Some(Self {
            timeout: InternalTransactionTimeout::Custom(timeout),
        })
    }

    /// Construct an infinite transaction timeout.
    ///
    /// This will instruct the server to never timeout the transaction.
    ///
    /// For server versions between 4.2 and 5.2 (inclusive), this will fall back to the default
    /// timeout configured on the server using the `db.transaction.timeout` setting
    /// (`dbms.transaction.timeout` before Neo4j 5.0).
    #[inline]
    pub fn none() -> Self {
        Self {
            timeout: InternalTransactionTimeout::None,
        }
    }

    #[inline]
    pub(crate) fn raw(&self) -> Option<i64> {
        self.timeout.raw()
    }
}

impl Default for TransactionTimeout {
    /// Construct a transaction timeout that uses the default timeout configured on the server.
    ///
    /// This corresponds to the timeout configured on the server using the
    /// `db.transaction.timeout` setting (`dbms.transaction.timeout` before Neo4j 5.0).
    #[inline]
    fn default() -> Self {
        Self {
            timeout: InternalTransactionTimeout::Default,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum InternalTransactionTimeout {
    None,
    Default,
    Custom(i64),
}

impl Default for InternalTransactionTimeout {
    fn default() -> Self {
        Self::Default
    }
}

impl InternalTransactionTimeout {
    #[inline]
    pub(crate) fn raw(&self) -> Option<i64> {
        match self {
            Self::None => Some(0),
            Self::Default => None,
            Self::Custom(timeout) => Some(*timeout),
        }
    }
}
