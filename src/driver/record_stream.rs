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
use std::ops::Deref;
use std::rc::Rc;

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

        self.connection.run(
            query, parameters, bookmarks, tx_timeout, tx_meta, mode, db, imp_user, callbacks,
        )?;
        self.pull()
    }

    pub fn consume() -> Result<Summary> {
        todo!()
    }

    fn pull(&mut self) -> Result<()> {
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
        self.connection.write_all()?;
        self.connection.read_all()
    }
}

impl<'a> Iterator for RecordStream<'a> {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        fn need_to_pull(listener: &Rc<RefCell<RecordListener>>) -> bool {
            let listener = listener.borrow();
            listener.buffer.is_empty() && listener.error.is_none() && listener.streaming
        }

        while need_to_pull(&self.listener) {
            if let Err(err) = self.pull() {
                self.listener.borrow_mut().streaming = false;
                return Some(Err(err));
            }
        }

        let mut listener = self.listener.borrow_mut();

        if let Some(record) = listener.buffer.pop_front() {
            Some(Ok(record))
        } else {
            listener.error.take().map(Err)
        }
    }
}

#[derive(Debug)]
enum RecordListenerState {
    Init,
    Pulling,
}

#[derive(Debug)]
struct RecordListener {
    streaming: bool,
    buffer: VecDeque<Record>,
    error: Option<Neo4jError>,
    keys: Option<Vec<String>>,
    state: RecordListenerState,
}

impl RecordListener {
    fn new() -> Self {
        Self {
            streaming: true,
            buffer: VecDeque::new(),
            error: None,
            keys: None,
            state: RecordListenerState::Init,
        }
    }
}

impl RecordListener {
    fn run_success_cb(&mut self, mut meta: BoltMeta) -> Result<()> {
        if self.keys.is_some() {
            return Ok(());
        }
        // TODO: t_first
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
        Ok(())
    }

    fn failure_cb(&mut self, meta: BoltMeta) -> Result<()> {
        self.error = Some(ServerError::from_meta(meta).into());
        Ok(())
    }

    fn record_cb(&mut self, fields: BoltRecordFields) -> Result<()> {
        self.buffer.push_back(Record { fields });
        Ok(())
    }

    fn pull_success_cb(&mut self, meta: BoltMeta) -> Result<()> {
        let Some(Value::Boolean(true)) = meta.get("has_more") else {
            self.streaming = false;
            return Ok(());
        };
        Ok(())
    }
}
