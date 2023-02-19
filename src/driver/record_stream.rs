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

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::mem;
use std::ops::Deref;
use std::rc::Rc;

use duplicate::duplicate_item;

use super::io::bolt::{BoltMeta, BoltRecordFields, BoltResponse, ResponseCallbacks, TcpBolt};
use crate::error::ServerError;
use crate::{Neo4jError, PackStreamSerialize, Record, Result, Summary, Value};

#[derive(Debug)]
pub struct RecordStream<'a> {
    connection: &'a mut TcpBolt,
    listener: Rc<RefCell<RecordListener>>,
}

impl<'a> RecordStream<'a> {
    pub fn new(connection: &'a mut TcpBolt) -> Self {
        Self {
            connection,
            listener: Rc::new(RefCell::new(RecordListener::new())),
        }
    }

    pub(crate) fn run<
        K1: Deref<Target = str> + Debug,
        S1: PackStreamSerialize,
        K2: Deref<Target = str> + Debug,
        S2: PackStreamSerialize,
    >(
        &mut self,
        query: &str,
        parameters: Option<&HashMap<K1, S1>>,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_meta: Option<&HashMap<K2, S2>>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<()> {
        let listener = Rc::downgrade(&self.listener);
        let mut callbacks = ResponseCallbacks::new().with_on_success(move |meta| {
            if let Some(listener) = listener.upgrade() {
                return listener.borrow_mut().run_success_cb(meta);
            }
            Ok(())
        });
        let listener = Rc::downgrade(&self.listener);
        callbacks = callbacks.with_on_failure(move |meta| {
            if let Some(listener) = listener.upgrade() {
                return listener.borrow_mut().failure_cb(meta);
            }
            Ok(())
        });

        assert!(!self.connection.has_buffered_message());
        assert!(!self.connection.expects_reply());

        if let Err(e) = self
            .connection
            .run(
                query, parameters, bookmarks, tx_timeout, tx_meta, mode, db, imp_user, callbacks,
            )
            .and_then(|_| self.pull(false))
            .and_then(|_| self.connection.write_all())
            .and_then(|_| self.connection.read_one())
        {
            let mut listener = self.listener.borrow_mut();
            listener.state = RecordListenerState::Done;
            return Err(e);
        };
        if let Err(err) = self.connection.read_all() {
            self.listener.borrow_mut().state = RecordListenerState::Error(err);
        }

        assert!(!self.connection.has_buffered_message());
        assert!(!self.connection.expects_reply());

        Ok(())
    }

    pub fn consume(&mut self) -> Result<Option<Summary>> {
        if self.listener.borrow().state.is_streaming() {
            let mut listener = self.listener.borrow_mut();
            listener.buffer.clear();
            listener.state = RecordListenerState::Discarding;
        }

        self.try_for_each(|e| e.map(drop))?;

        Ok(self.listener.borrow_mut().summary.take())
    }

    pub(crate) fn into_bookmarks(self) -> Vec<String> {
        Rc::try_unwrap(self.listener)
            .unwrap()
            .into_inner()
            .bookmarks
    }

    fn pull(&mut self, flush: bool) -> Result<()> {
        let listener = Rc::downgrade(&self.listener);
        let mut callbacks = ResponseCallbacks::new().with_on_success(move |meta| {
            if let Some(listener) = listener.upgrade() {
                return listener.borrow_mut().pull_success_cb(meta);
            }
            Ok(())
        });
        let listener = Rc::downgrade(&self.listener);
        callbacks = callbacks.with_on_record(move |data| {
            if let Some(listener) = listener.upgrade() {
                return listener.borrow_mut().record_cb(data);
            }
            Ok(())
        });
        self.connection.pull(1000, -1, callbacks)?;
        if flush {
            self.connection.write_all()?;
            self.connection.read_all()?;
        }
        Ok(())
    }

    fn discard(&mut self, flush: bool) -> Result<()> {
        let listener = Rc::downgrade(&self.listener);
        let callbacks = ResponseCallbacks::new().with_on_success(move |meta| {
            if let Some(listener) = listener.upgrade() {
                return listener.borrow_mut().pull_success_cb(meta);
            }
            Ok(())
        });
        self.connection.discard(-1, -1, callbacks)?;
        if flush {
            self.connection.write_all()?;
            self.connection.read_all()?;
        }
        Ok(())
    }
}

impl<'a> Iterator for RecordStream<'a> {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        fn need_to_pull(listener: &Rc<RefCell<RecordListener>>) -> bool {
            let listener = listener.borrow();
            listener.buffer.is_empty() && listener.state.is_streaming()
        }

        fn need_to_discard(listener: &Rc<RefCell<RecordListener>>) -> bool {
            let listener = listener.borrow();
            listener.buffer.is_empty() && listener.state.is_discarding()
        }

        loop {
            if let Some(record) = self.listener.borrow_mut().buffer.pop_front() {
                return Some(Ok(record));
            }
            if need_to_pull(&self.listener) {
                if let Err(err) = self.pull(true) {
                    self.listener.borrow_mut().set_error(err);
                }
            } else if need_to_discard(&self.listener) {
                if let Err(err) = self.discard(true) {
                    self.listener.borrow_mut().set_error(err);
                }
            }
            let mut listener = self.listener.borrow_mut();
            match listener.state {
                RecordListenerState::Error(_) => {
                    let mut state = RecordListenerState::Done;
                    mem::swap(&mut listener.state, &mut state);
                    match state {
                        RecordListenerState::Error(e) => return Some(Err(e)),
                        _ => panic!("checked state to be error above"),
                    }
                }
                RecordListenerState::Done => return None,
                _ => {}
            }
        }
    }
}

#[derive(Debug)]
enum RecordListenerState {
    Streaming,
    Discarding,
    Error(Neo4jError),
    Done,
}

impl RecordListenerState {
    #[duplicate_item(
        fn_name            variant;
        [ is_streaming ]   [ Streaming ];
        [ is_discarding ]  [ Discarding ];
        [ is_error ]       [ Error(_) ];
        [ is_done ]        [ Done ];
    )]
    pub fn fn_name(&self) -> bool {
        matches!(self, RecordListenerState::variant)
    }
}

#[derive(Debug)]
struct RecordListener {
    buffer: VecDeque<Record>,
    keys: Option<Vec<String>>,
    state: RecordListenerState,
    summary: Option<Summary>,
    bookmarks: Vec<String>,
}

impl RecordListener {
    fn new() -> Self {
        Self {
            buffer: VecDeque::new(),
            keys: None,
            state: RecordListenerState::Streaming,
            summary: Some(Summary::default()),
            bookmarks: Vec::new(),
        }
    }
}

impl RecordListener {
    fn run_success_cb(&mut self, mut meta: BoltMeta) -> Result<()> {
        if self.keys.is_some() {
            return Ok(());
        }
        // TODO: qid (when transaction support)
        let Some(fields) = meta.remove("fields") else {
            return Err(Neo4jError::ProtocolError {
                message: "SUCCESS after RUN did not contain 'fields'".into()
            });
        };
        let Value::List(fields) = fields else {
            return Err(Neo4jError::ProtocolError {
                message: "SUCCESS after RUN 'fields' was not a list".into()});
        };
        let fields = fields
            .into_iter()
            .map(|field| match field {
                Value::String(field) => Ok(field),
                _ => Err(Neo4jError::ProtocolError {
                    message: "SUCCESS after RUN 'fields' was not a list".into(),
                }),
            })
            .collect::<Result<_>>()?;
        self.keys = Some(fields);
        if let Some(summary) = self.summary.as_mut() {
            summary.load_run_meta(&mut meta)?
        }

        Ok(())
    }

    fn failure_cb(&mut self, meta: BoltMeta) -> Result<()> {
        self.state = RecordListenerState::Error(ServerError::from_meta(meta).into());
        self.summary = None;
        Ok(())
    }

    fn record_cb(&mut self, fields: BoltRecordFields) -> Result<()> {
        self.buffer.push_back(Record { fields });
        Ok(())
    }

    fn pull_success_cb(&mut self, mut meta: BoltMeta) -> Result<()> {
        let Some(Value::Boolean(true)) = meta.remove("has_more") else {
            self.state = RecordListenerState::Done;
            if let Some(summary) = self.summary.as_mut() {
                summary.load_pull_meta(&mut meta)?
            }
            return Ok(());
        };
        Ok(())
    }

    fn set_error(&mut self, error: Neo4jError) {
        self.state = RecordListenerState::Error(error);
        self.summary = None
    }
}
