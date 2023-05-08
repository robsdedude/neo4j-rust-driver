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

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use flume;
use flume::{Receiver, Sender};

use crate::driver::{Driver, Record, RecordStream, RoutingControl};
use crate::session::SessionConfig;
use crate::summary::Summary;
use crate::{Neo4jError, ValueSend};

use super::backend_id::Generator;
use super::errors::TestKitError;
use super::BackendId;

#[derive(Debug)]
pub(crate) struct SessionHolder {
    tx_req: Sender<Command>,
    rx_res: Receiver<CommandResult>,
    join_handle: Option<JoinHandle<()>>,
}

impl SessionHolder {
    pub(crate) fn new(
        id_generator: Generator,
        driver: Arc<Driver>,
        auto_commit_access_mode: RoutingControl,
        config: SessionConfig,
    ) -> Self {
        let (tx_req, rx_req) = flume::unbounded();
        let (tx_res, rx_res) = flume::unbounded();
        let handle = thread::spawn(move || {
            let mut runner = SessionHolderRunner {
                id_generator,
                auto_commit_access_mode,
                config,
                rx_req,
                tx_res,
                driver,
                record_buffers: HashMap::new(),
            };
            runner.run();
        });
        Self {
            tx_req,
            rx_res,
            join_handle: Some(handle),
        }
    }

    pub(crate) fn auto_commit(&self, args: AutoCommit) -> AutoCommitResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::AutoCommit(result) => result,
            res => panic!("expected CommandResult::AutoCommit, found {:?}", res),
        }
    }

    pub(crate) fn result_next(&self, args: ResultNext) -> ResultNextResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultNext(result) => result,
            res => panic!("expected CommandResult::ResultNext, found {:?}", res),
        }
    }

    pub(crate) fn result_consume(&self, args: ResultConsume) -> ResultConsumeResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultConsume(result) => result,
            res => panic!("expected CommandResult::ResultConsume, found {:?}", res),
        }
    }

    pub(crate) fn close(&mut self) {
        let Some(handle) = self.join_handle.take() else {
            return;
        };
        self.tx_req.send(Command::Close).unwrap();
        handle.join().unwrap();
    }
}

impl Drop for SessionHolder {
    fn drop(&mut self) {
        self.close()
    }
}

#[derive(Debug)]
struct SessionHolderRunner {
    id_generator: Generator,
    auto_commit_access_mode: RoutingControl,
    config: SessionConfig,
    driver: Arc<Driver>,
    rx_req: Receiver<Command>,
    tx_res: Sender<CommandResult>,
    record_buffers: HashMap<BackendId, RecordBuffer>,
}

impl SessionHolderRunner {
    fn run(&mut self) {
        let mut session = self.driver.session(&self.config);
        let mut buffered_command = None;
        // let mut sessions = HashMap::new();
        loop {
            let _ = buffered_command.get_or_insert_with(|| self.rx_req.recv().unwrap());
            match buffered_command.take().unwrap() {
                Command::AutoCommit(AutoCommit {
                    session_id: _,
                    query,
                    params,
                    tx_meta,
                    timeout,
                }) => {
                    let mut auto_commit = session.auto_commit(query);
                    auto_commit = auto_commit.with_routing_control(self.auto_commit_access_mode);
                    if let Some(timeout) = timeout {
                        auto_commit = auto_commit.with_tx_timeout(timeout);
                    }
                    if let Some(tx_meta) = tx_meta {
                        auto_commit = auto_commit.with_tx_meta(tx_meta)
                    }
                    let mut started_receiver = false;
                    let auto_commit = {
                        let started_receiver = &mut started_receiver;
                        let record_buffers = &mut self.record_buffers;
                        let tx_res = &self.tx_res;
                        auto_commit.with_receiver(|stream| {
                            *started_receiver = true;
                            let keys = stream.keys();
                            let id = self.id_generator.next_id();
                            record_buffers.insert(id, RecordBuffer::new());
                            tx_res
                                .send(
                                    AutoCommitResult {
                                        result: Ok((id, keys)),
                                    }
                                    .into(),
                                )
                                .unwrap();
                            loop {
                                let _ = buffered_command
                                    .get_or_insert_with(|| self.rx_req.recv().unwrap());
                                match buffered_command.take().unwrap() {
                                    Command::ResultNext(ResultNext { result_id })
                                        if result_id == id =>
                                    {
                                        self.tx_res
                                            .send(
                                                ResultNextResult {
                                                    result: Ok(stream.next()),
                                                }
                                                .into(),
                                            )
                                            .unwrap();
                                    }
                                    Command::ResultNext(ResultNext { result_id }) => {
                                        Self::send_record_from_buffer(
                                            record_buffers,
                                            result_id,
                                            tx_res,
                                        )
                                    }
                                    Command::ResultConsume(ResultConsume { result_id })
                                        if result_id == id =>
                                    {
                                        record_buffers.get_mut(&id).unwrap().buffer(stream);
                                        Self::send_summary_from_buffer(record_buffers, id, tx_res);
                                        return Ok(());
                                    }
                                    Command::ResultConsume(ResultConsume { result_id }) => {
                                        Self::send_summary_from_buffer(
                                            record_buffers,
                                            result_id,
                                            tx_res,
                                        )
                                    }
                                    command @ (Command::AutoCommit(_) | Command::Close) => {
                                        let _ = buffered_command.insert(command);
                                        record_buffers.get_mut(&id).unwrap().buffer(stream);
                                        return Ok(());
                                    }
                                }
                            }
                        })
                    };
                    let res = if let Some(params) = params {
                        auto_commit.run_with_parameters(params)
                    } else {
                        auto_commit.run()
                    };
                    if let Err(e) = res {
                        if started_receiver {
                            panic!("receiver is infallible and exhausts its stream");
                        }
                        self.tx_res
                            .send(
                                AutoCommitResult {
                                    result: Err(e.into()),
                                }
                                .into(),
                            )
                            .unwrap();
                    }
                }

                Command::ResultNext(ResultNext { result_id }) => {
                    Self::send_record_from_buffer(&mut self.record_buffers, result_id, &self.tx_res)
                }

                Command::ResultConsume(ResultConsume { result_id }) => {
                    Self::send_summary_from_buffer(
                        &mut self.record_buffers,
                        result_id,
                        &self.tx_res,
                    )
                }

                Command::Close => break,
            }
        }
    }

    fn send_record_from_buffer(
        record_buffers: &mut HashMap<BackendId, RecordBuffer>,
        id: BackendId,
        tx_res: &Sender<CommandResult>,
    ) {
        match record_buffers.get_mut(&id) {
            None => tx_res
                .send(
                    ResultNextResult {
                        result: Err(TestKitError::backend_err(format!(
                            "unknown result id {id} in session"
                        ))),
                    }
                    .into(),
                )
                .unwrap(),
            Some(buffer) => tx_res
                .send(
                    ResultNextResult {
                        result: Ok(buffer.records.pop_front()),
                    }
                    .into(),
                )
                .unwrap(),
        }
    }

    fn send_summary_from_buffer(
        record_buffers: &mut HashMap<BackendId, RecordBuffer>,
        id: BackendId,
        tx_res: &Sender<CommandResult>,
    ) {
        match record_buffers.get_mut(&id) {
            None => tx_res
                .send(
                    ResultConsumeResult {
                        result: Err(TestKitError::backend_err(format!(
                            "unknown result id {id} in session"
                        ))),
                    }
                    .into(),
                )
                .unwrap(),
            Some(buffer) => tx_res
                .send(
                    ResultConsumeResult {
                        result: buffer.consume(),
                    }
                    .into(),
                )
                .unwrap(),
        }
    }
}

#[derive(Debug)]
struct RecordBuffer {
    records: VecDeque<Result<Record, Neo4jError>>,
    summary: Option<Summary>,
}

impl RecordBuffer {
    fn new() -> Self {
        RecordBuffer {
            records: VecDeque::new(),
            summary: None,
        }
    }

    fn from_stream(stream: &mut RecordStream) -> Self {
        let mut buffer = Self::new();
        buffer.buffer(stream);
        buffer
    }

    fn buffer(&mut self, stream: &mut RecordStream) {
        stream.for_each(|rec| self.records.push_back(rec));
        self.summary = stream
            .consume()
            .expect("result stream exhausted above => cannot fail on consume");
    }

    fn consume(&mut self) -> Result<Result<Summary, Neo4jError>, TestKitError> {
        let mut records = Default::default();
        mem::swap(&mut self.records, &mut records);
        if let Some(Err(e)) = records.into_iter().rev().next() {
            return Ok(Err(e));
        } else {
            assert!(self.summary.is_some());
        }
        match &self.summary {
            None => Err(TestKitError::backend_err(
                "cannot receive summary of a failed record stream",
            )),
            Some(summary) => Ok(Ok(summary.clone())),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Command {
    AutoCommit(AutoCommit),
    ResultNext(ResultNext),
    ResultConsume(ResultConsume),
    Close,
}

#[derive(Debug)]
pub(crate) enum CommandResult {
    AutoCommit(AutoCommitResult),
    ResultNext(ResultNextResult),
    ResultConsume(ResultConsumeResult),
}

#[derive(Debug)]
pub(crate) struct AutoCommit {
    pub(crate) session_id: BackendId,
    pub(crate) query: String,
    pub(crate) params: Option<HashMap<String, ValueSend>>,
    pub(crate) tx_meta: Option<HashMap<String, ValueSend>>,
    pub(crate) timeout: Option<i64>,
}

impl From<AutoCommit> for Command {
    fn from(c: AutoCommit) -> Self {
        Command::AutoCommit(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct AutoCommitResult {
    pub(crate) result: Result<(BackendId, Vec<Arc<String>>), TestKitError>,
}

impl From<AutoCommitResult> for CommandResult {
    fn from(r: AutoCommitResult) -> Self {
        CommandResult::AutoCommit(r)
    }
}

#[derive(Debug)]
pub(crate) struct ResultNext {
    pub(crate) result_id: BackendId,
}

impl From<ResultNext> for Command {
    fn from(c: ResultNext) -> Self {
        Command::ResultNext(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct ResultNextResult {
    pub(crate) result: Result<Option<Result<Record, Neo4jError>>, TestKitError>,
}

impl From<ResultNextResult> for CommandResult {
    fn from(r: ResultNextResult) -> Self {
        CommandResult::ResultNext(r)
    }
}

#[derive(Debug)]
pub(crate) struct ResultConsume {
    pub(crate) result_id: BackendId,
}

impl From<ResultConsume> for Command {
    fn from(c: ResultConsume) -> Self {
        Command::ResultConsume(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct ResultConsumeResult {
    pub(crate) result: Result<Result<Summary, Neo4jError>, TestKitError>,
}

impl From<ResultConsumeResult> for CommandResult {
    fn from(r: ResultConsumeResult) -> Self {
        CommandResult::ResultConsume(r)
    }
}
