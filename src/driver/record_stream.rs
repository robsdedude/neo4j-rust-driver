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

use super::io::bolt::ResponseCallbacks;
use super::io::bolt::TcpBolt;
use crate::{PackStreamSerialize, Record, Result, Summary};

#[derive(Debug)]
pub struct RecordStream<'a> {
    connection: &'a mut TcpBolt,
    listener: Rc<RefCell<RecordListener>>,
}

impl<'a> Iterator for RecordStream<'a> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
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
        let listener = Rc::clone(&self.listener);
        let callbacks = ResponseCallbacks::new().with_on_success(move |meta| {
            listener.borrow_mut().streaming = false;
            Ok(())
        });

        self.connection.run(
            query, parameters, bookmarks, tx_timeout, tx_meta, mode, db, imp_user, callbacks,
        )?;
        self.connection.pull(1000, -1)?;
        self.connection.write_all()?;
        self.connection.read_all()
    }

    pub fn consume() -> Result<Summary> {
        todo!()
    }
}

#[derive(Debug)]
struct RecordListener {
    streaming: bool,
    buffer: VecDeque<Record>,
}

impl RecordListener {
    fn new() -> Self {
        Self {
            streaming: true,
            buffer: VecDeque::new(),
        }
    }
}
