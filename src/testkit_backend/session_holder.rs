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
use log::warn;

use crate::driver::record_stream::{GetSingleRecordError, RecordStream};
use crate::driver::transaction::TransactionRecordStream;
use crate::driver::{Driver, Record, RoutingControl};
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

    pub(crate) fn begin_transaction(&self, args: BeginTransaction) -> BeginTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::BeginTransaction(result) => result,
            res => panic!("expected CommandResult::BeginTransaction, found {:?}", res),
        }
    }

    pub(crate) fn transaction_run(&self, args: TransactionRun) -> TransactionRunResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::TransactionRun(result) => result,
            res => panic!("expected CommandResult::TransactionRun, found {:?}", res),
        }
    }

    pub(crate) fn commit_transaction(&self, args: CommitTransaction) -> CommitTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::CommitTransaction(result) => result,
            res => panic!("expected CommandResult::CommitTransaction, found {:?}", res),
        }
    }

    pub(crate) fn rollback_transaction(
        &self,
        args: RollbackTransaction,
    ) -> RollbackTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::RollbackTransaction(result) => result,
            res => panic!(
                "expected CommandResult::RollbackTransaction, found {:?}",
                res
            ),
        }
    }

    pub(crate) fn close_transaction(&self, args: CloseTransaction) -> CloseTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::CloseTransaction(result) => result,
            res => panic!("expected CommandResult::CloseTransaction, found {:?}", res),
        }
    }

    pub(crate) fn result_next(&self, args: ResultNext) -> ResultNextResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultNext(result) => result,
            res => panic!("expected CommandResult::ResultNext, found {:?}", res),
        }
    }

    pub(crate) fn result_single(&self, args: ResultSingle) -> ResultSingleResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultSingle(result) => result,
            res => panic!("expected CommandResult::ResultSingle, found {:?}", res),
        }
    }

    pub(crate) fn result_consume(&self, args: ResultConsume) -> ResultConsumeResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultConsume(result) => result,
            res => panic!("expected CommandResult::ResultConsume, found {:?}", res),
        }
    }

    pub(crate) fn close(&mut self) -> CloseResult {
        let Some(handle) = self.join_handle.take() else {
            return CloseResult{result: Ok(())};
        };
        self.tx_req.send(Command::Close).unwrap();
        let res = match self.rx_res.recv().unwrap() {
            CommandResult::Close(result) => result,
            res => panic!("expected CommandResult::Close, found {:?}", res),
        };
        handle.join().unwrap();
        res
    }
}

impl Drop for SessionHolder {
    fn drop(&mut self) {
        if let CloseResult { result: Err(e) } = self.close() {
            warn!("Ignored session closure error on drop: {e:?}");
        }
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
}

impl SessionHolderRunner {
    fn run(&mut self) {
        let mut session = self.driver.session(&self.config);
        let mut record_holders: HashMap<BackendId, RecordBuffer> = Default::default();
        let mut known_transactions: HashMap<BackendId, TxFailState> = Default::default();
        let mut buffered_command = None;
        loop {
            match Self::next_command(&self.rx_req, &mut buffered_command) {
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
                        if timeout == 0 {
                            auto_commit = auto_commit.without_tx_timeout();
                        } else {
                            auto_commit = match auto_commit.with_tx_timeout(timeout) {
                                Ok(auto_commit) => auto_commit,
                                Err(e) => {
                                    let msg = AutoCommitResult {
                                        result: Err(e.into()),
                                    };
                                    self.tx_res.send(msg.into()).unwrap();
                                    continue;
                                }
                            };
                        }
                    }
                    if let Some(tx_meta) = tx_meta {
                        auto_commit = auto_commit.with_tx_meta(tx_meta)
                    }
                    let mut started_receiver = false;
                    let known_transactions = &known_transactions;
                    let auto_commit = {
                        let started_receiver = &mut started_receiver;
                        let record_holders = &mut record_holders;
                        let tx_res = &self.tx_res;
                        auto_commit.with_receiver(|stream| {
                            *started_receiver = true;
                            let keys = stream.keys();
                            let id = self.id_generator.next_id();
                            record_holders.insert(id, RecordBuffer::new_auto_commit());
                            tx_res
                                .send(
                                    AutoCommitResult {
                                        result: Ok((id, keys)),
                                    }
                                    .into(),
                                )
                                .unwrap();
                            loop {
                                match Self::next_command(&self.rx_req, &mut buffered_command) {
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
                                        Self::send_record_from_holders(
                                            record_holders,
                                            result_id,
                                            tx_res,
                                        )
                                    }
                                    Command::ResultSingle(ResultSingle { result_id })
                                        if result_id == id =>
                                    {
                                        self.tx_res
                                            .send(
                                                ResultSingleResult {
                                                    result: Ok(stream
                                                        .single()
                                                        .map_err(Into::into)
                                                        .and_then(Into::into)),
                                                }
                                                .into(),
                                            )
                                            .unwrap();
                                    }
                                    Command::ResultSingle(ResultSingle { result_id }) => {
                                        Self::send_record_single_from_holders(
                                            record_holders,
                                            result_id,
                                            tx_res,
                                        )
                                    }
                                    Command::ResultConsume(ResultConsume { result_id })
                                        if result_id == id =>
                                    {
                                        record_holders
                                            .get_mut(&id)
                                            .unwrap()
                                            .buffer_record_stream(stream);
                                        Self::send_summary_from_holders(record_holders, id, tx_res);
                                        return Ok(());
                                    }
                                    Command::ResultConsume(ResultConsume { result_id }) => {
                                        Self::send_summary_from_holders(
                                            record_holders,
                                            result_id,
                                            tx_res,
                                        )
                                    }
                                    Command::TransactionRun(command) => {
                                        command.default_response(tx_res, known_transactions);
                                    }
                                    Command::CommitTransaction(command) => {
                                        command.default_response(&self.tx_res, known_transactions)
                                    }
                                    Command::RollbackTransaction(command) => {
                                        command.default_response(&self.tx_res, known_transactions)
                                    }
                                    Command::CloseTransaction(command) => {
                                        command.default_response(&self.tx_res, known_transactions)
                                    }
                                    command @ (Command::AutoCommit(_)
                                    | Command::BeginTransaction(_)
                                    | Command::Close) => {
                                        let _ = buffered_command.insert(command);
                                        record_holders
                                            .get_mut(&id)
                                            .unwrap()
                                            .buffer_record_stream(stream);
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
                            let command = buffered_command
                                .take()
                                .unwrap_or_else(|| panic!("about to swallow closure error: {e:?}"));
                            command.reply_error(e.into(), &self.tx_res);
                            if matches!(command, Command::Close) {
                                return;
                            }
                        } else {
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
                }

                Command::BeginTransaction(BeginTransaction {
                    session_id: _,
                    tx_meta,
                    timeout,
                }) => {
                    let mut transaction = session.transaction();
                    transaction = transaction.with_routing_control(self.auto_commit_access_mode);
                    if let Some(timeout) = timeout {
                        if timeout == 0 {
                            transaction = transaction.without_tx_timeout();
                        } else {
                            transaction = match transaction.with_tx_timeout(timeout) {
                                Ok(transaction) => transaction,
                                Err(e) => {
                                    let msg = BeginTransactionResult {
                                        result: Err(e.into()),
                                    };
                                    self.tx_res.send(msg.into()).unwrap();
                                    continue;
                                }
                            };
                        }
                    }
                    if let Some(tx_meta) = tx_meta {
                        transaction = transaction.with_tx_meta(tx_meta)
                    }
                    let mut started_receiver = false;
                    let res = {
                        let started_receiver = &mut started_receiver;
                        let record_holders = &mut record_holders;
                        let known_transactions = &mut known_transactions;
                        let tx_res = &self.tx_res;
                        transaction.run(|transaction| {
                            *started_receiver = true;
                            let id = self.id_generator.next_id();
                            known_transactions.insert(id, TxFailState::Passed);

                            tx_res
                                .send(BeginTransactionResult { result: Ok(Ok(id)) }.into())
                                .unwrap();

                            let mut streams: HashMap<BackendId, Option<TransactionRecordStream>> =
                                Default::default();
                            let mut summaries: HashMap<BackendId, Summary> = Default::default();
                            loop {
                                match Self::next_command(&self.rx_req, &mut buffered_command) {
                                    Command::TransactionRun(command) => {
                                        if command.transaction_id != id {
                                            command.default_response(tx_res, known_transactions);
                                            continue;
                                        }
                                        let TransactionRun { params, query, .. } = command;
                                        let result = match params {
                                            None => transaction.run(query),
                                            Some(params) => {
                                                transaction.run_with_parameters(query, params)
                                            }
                                        };
                                        match result {
                                            Ok(result) => {
                                                let keys = result.keys();
                                                let result_id = self.id_generator.next_id();
                                                streams.insert(result_id, Some(result));
                                                let msg = TransactionRunResult {
                                                    result: Ok(Ok((result_id, keys))),
                                                };
                                                tx_res.send(msg.into()).unwrap();
                                            }
                                            Err(err) => {
                                                known_transactions.insert(id, TxFailState::Failed);
                                                let msg = TransactionRunResult {
                                                    result: Ok(Err(err)),
                                                };
                                                tx_res.send(msg.into()).unwrap();
                                                break;
                                            }
                                        }
                                    }
                                    Command::ResultNext(ResultNext { result_id }) => {
                                        match streams.get_mut(&result_id) {
                                            None => Self::send_record_from_holders(
                                                record_holders,
                                                result_id,
                                                tx_res,
                                            ),
                                            Some(stream) => {
                                                let result = match stream {
                                                    None => Err(result_consumed_error()),
                                                    Some(stream) => Ok({
                                                        let res = stream.next();
                                                        if matches!(res, Some(Err(_))) {
                                                            known_transactions
                                                                .insert(id, TxFailState::Failed);
                                                        }
                                                        res
                                                    }),
                                                };
                                                let msg = ResultNextResult { result };
                                                tx_res.send(msg.into()).unwrap();
                                            }
                                        }
                                    }
                                    Command::ResultSingle(ResultSingle { result_id }) => {
                                        match streams.get_mut(&result_id) {
                                            None => Self::send_record_single_from_holders(
                                                record_holders,
                                                result_id,
                                                tx_res,
                                            ),
                                            Some(stream) => {
                                                let result = match stream {
                                                    None => Err(result_consumed_error()),
                                                    Some(stream) => Ok(stream
                                                        .single()
                                                        .map_err(Into::into)
                                                        .and_then(|r| {
                                                            if r.is_err() {
                                                                known_transactions.insert(
                                                                    id,
                                                                    TxFailState::Failed,
                                                                );
                                                            }
                                                            r
                                                        })),
                                                };
                                                let msg = ResultSingleResult { result };
                                                tx_res.send(msg.into()).unwrap();
                                            }
                                        }
                                    }
                                    Command::ResultConsume(ResultConsume { result_id }) => {
                                        if let Some(stream) = streams.get_mut(&result_id) {
                                            if let Some(stream) = stream.take() {
                                                match stream.consume() {
                                                    Ok(summary) => {
                                                        summaries.insert(
                                                            result_id,
                                                            summary.expect(
                                                                "will only ever fetch summary once",
                                                            ),
                                                        );
                                                    }
                                                    Err(err) => {
                                                        known_transactions
                                                            .insert(id, TxFailState::Failed);
                                                        tx_res
                                                            .send(
                                                                ResultConsumeResult {
                                                                    result: Ok(Err(err)),
                                                                }
                                                                .into(),
                                                            )
                                                            .unwrap();
                                                        continue;
                                                    }
                                                }
                                            }
                                        }
                                        if let Some(summary) = summaries.get(&result_id) {
                                            let msg = ResultConsumeResult {
                                                result: Ok(Ok(summary.clone())),
                                            };
                                            tx_res.send(msg.into()).unwrap();
                                        } else {
                                            Self::send_summary_from_holders(
                                                record_holders,
                                                result_id,
                                                tx_res,
                                            );
                                        }
                                    }
                                    Command::CommitTransaction(command) => {
                                        if command.transaction_id != id {
                                            command.default_response(tx_res, known_transactions);
                                            continue;
                                        }
                                        streams.into_keys().for_each(|id| {
                                            record_holders
                                                .insert(id, RecordBuffer::new_transaction());
                                        });
                                        let result = transaction.commit();
                                        if result.is_err() {
                                            known_transactions.insert(id, TxFailState::Failed);
                                        }
                                        command.real_response(result, tx_res);
                                        return Ok(());
                                    }
                                    Command::RollbackTransaction(command) => {
                                        if command.transaction_id != id {
                                            command.default_response(tx_res, known_transactions);
                                            continue;
                                        }
                                        streams.into_keys().for_each(|id| {
                                            record_holders
                                                .insert(id, RecordBuffer::new_transaction());
                                        });
                                        let result = transaction.rollback();
                                        if result.is_err() {
                                            known_transactions.insert(id, TxFailState::Failed);
                                        }
                                        command.real_response(result, tx_res);
                                        return Ok(());
                                    }
                                    Command::CloseTransaction(command) => {
                                        if command.transaction_id != id {
                                            command.default_response(tx_res, known_transactions);
                                            continue;
                                        }
                                        streams.into_keys().for_each(|id| {
                                            record_holders
                                                .insert(id, RecordBuffer::new_transaction());
                                        });
                                        let result = transaction.rollback();
                                        if result.is_err() {
                                            known_transactions.insert(id, TxFailState::Failed);
                                        }
                                        command.real_response(result, tx_res);
                                        return Ok(());
                                    }
                                    command @ (Command::BeginTransaction(_)
                                    | Command::AutoCommit(_)
                                    | Command::Close) => {
                                        let _ = buffered_command.insert(command);
                                        break;
                                    }
                                }
                            }
                            streams.into_keys().for_each(|id| {
                                record_holders.insert(id, RecordBuffer::new_transaction());
                            });
                            Ok(())
                        })
                    };
                    if let Err(e) = res {
                        if started_receiver {
                            let command = buffered_command
                                .take()
                                .unwrap_or_else(|| panic!("about to swallow closure error: {e:?}"));
                            command.reply_error(e.into(), &self.tx_res);
                            if matches!(command, Command::Close) {
                                return;
                            }
                        } else {
                            self.tx_res
                                .send(
                                    BeginTransactionResult {
                                        result: Err(e.into()),
                                    }
                                    .into(),
                                )
                                .unwrap();
                        }
                    }
                }

                Command::TransactionRun(command) => {
                    command.default_response(&self.tx_res, &known_transactions)
                }

                Command::CommitTransaction(command) => {
                    command.default_response(&self.tx_res, &known_transactions)
                }

                Command::RollbackTransaction(command) => {
                    command.default_response(&self.tx_res, &known_transactions)
                }

                Command::CloseTransaction(command) => {
                    command.default_response(&self.tx_res, &known_transactions)
                }

                Command::ResultNext(ResultNext { result_id }) => {
                    Self::send_record_from_holders(&mut record_holders, result_id, &self.tx_res)
                }

                Command::ResultSingle(ResultSingle { result_id }) => {
                    Self::send_record_single_from_holders(
                        &mut record_holders,
                        result_id,
                        &self.tx_res,
                    )
                }

                Command::ResultConsume(ResultConsume { result_id }) => {
                    Self::send_summary_from_holders(&mut record_holders, result_id, &self.tx_res)
                }

                Command::Close => {
                    self.tx_res
                        .send(CloseResult { result: Ok(()) }.into())
                        .unwrap();
                    return;
                }
            }
        }
    }

    fn send_record_from_holders(
        record_holders: &mut HashMap<BackendId, RecordBuffer>,
        id: BackendId,
        tx_res: &Sender<CommandResult>,
    ) {
        let msg = match record_holders.get_mut(&id) {
            None => ResultNextResult {
                result: Err(TestKitError::backend_err(format!(
                    "unknown result id {id} in session"
                ))),
            }
            .into(),
            Some(buffer) => ResultNextResult {
                result: buffer.next(),
            }
            .into(),
        };
        tx_res.send(msg).unwrap();
    }

    fn send_record_single_from_holders(
        record_holders: &mut HashMap<BackendId, RecordBuffer>,
        id: BackendId,
        tx_res: &Sender<CommandResult>,
    ) {
        let msg = match record_holders.get_mut(&id) {
            None => ResultSingleResult {
                result: Err(TestKitError::backend_err(format!(
                    "unknown result id {id} in session"
                ))),
            }
            .into(),
            Some(buffer) => ResultSingleResult {
                result: buffer.single(),
            }
            .into(),
        };
        tx_res.send(msg).unwrap();
    }

    fn send_summary_from_holders(
        record_holders: &mut HashMap<BackendId, RecordBuffer>,
        id: BackendId,
        tx_res: &Sender<CommandResult>,
    ) {
        let msg = match record_holders.get_mut(&id) {
            None => ResultConsumeResult {
                result: Err(TestKitError::backend_err(format!(
                    "unknown result id {id} in session"
                ))),
            }
            .into(),
            Some(buffer) => ResultConsumeResult {
                result: buffer.consume(),
            }
            .into(),
        };
        tx_res.send(msg).unwrap();
    }

    fn next_command(rx_req: &Receiver<Command>, buffered_command: &mut Option<Command>) -> Command {
        let _ = buffered_command.get_or_insert_with(|| rx_req.recv().unwrap());
        buffered_command.take().unwrap()
    }
}

fn result_out_of_scope_error() -> TestKitError {
    TestKitError::DriverError {
        error_type: String::from("ResultOutOfScope"),
        msg: String::from("The record stream's transaction has been closed"),
        code: None,
    }
}

fn result_consumed_error() -> TestKitError {
    TestKitError::DriverError {
        error_type: String::from("ResultConsumed"),
        msg: String::from("The record stream's has been consumed"),
        code: None,
    }
}

fn transaction_out_of_scope_error() -> TestKitError {
    TestKitError::DriverError {
        error_type: String::from("TransactionOutOfScope"),
        msg: String::from("The transaction has been closed"),
        code: None,
    }
}

#[derive(Debug)]
enum RecordBuffer {
    AutoCommit {
        records: VecDeque<Result<Record, Neo4jError>>,
        summary: Option<Summary>,
        consumed: bool,
    },
    Transaction,
}

impl RecordBuffer {
    fn new_auto_commit() -> Self {
        RecordBuffer::AutoCommit {
            records: Default::default(),
            summary: Default::default(),
            consumed: false,
        }
    }

    fn new_transaction() -> Self {
        RecordBuffer::Transaction
    }

    fn from_record_stream(stream: &mut RecordStream) -> Self {
        let mut buffer = Self::new_auto_commit();
        buffer.buffer_record_stream(stream);
        buffer
    }

    fn buffer_record_stream(&mut self, stream: &mut RecordStream) {
        match self {
            RecordBuffer::AutoCommit {
                records, summary, ..
            } => {
                stream.for_each(|rec| records.push_back(rec));
                *summary = stream
                    .consume()
                    .expect("result stream exhausted above => cannot fail on consume");
            }
            RecordBuffer::Transaction { .. } => {
                panic!("cannot buffer record stream in transaction")
            }
        }
    }

    fn next(&mut self) -> Result<Option<Result<Record, Neo4jError>>, TestKitError> {
        match self {
            RecordBuffer::AutoCommit {
                records, consumed, ..
            } => {
                if *consumed {
                    Err(result_consumed_error())
                } else {
                    Ok(records.pop_front())
                }
            }
            RecordBuffer::Transaction { .. } => Err(result_out_of_scope_error()),
        }
    }

    fn single(&mut self) -> Result<Result<Record, Neo4jError>, TestKitError> {
        match self {
            RecordBuffer::AutoCommit {
                records, consumed, ..
            } => {
                if *consumed {
                    Err(result_consumed_error())
                } else {
                    match records.pop_front() {
                        None => Ok(Err(GetSingleRecordError::NoRecords.into())),
                        Some(result) => {
                            if result.is_ok() {
                                self.consume().unwrap()?;
                            }
                            Ok(result)
                        }
                    }
                }
            }
            RecordBuffer::Transaction { .. } => Err(result_out_of_scope_error()),
        }
    }

    fn consume(&mut self) -> Result<Result<Summary, Neo4jError>, TestKitError> {
        match self {
            RecordBuffer::AutoCommit {
                records,
                summary,
                consumed,
            } => {
                *consumed = true;
                let mut swapped_records = Default::default();
                mem::swap(records, &mut swapped_records);
                if let Err(e) = swapped_records.into_iter().try_for_each(|r| {
                    drop(r?);
                    Ok(())
                }) {
                    return Ok(Err(e));
                } else {
                    assert!(summary.is_some());
                }
                match summary {
                    None => Err(TestKitError::backend_err(
                        "cannot receive summary of a failed record stream",
                    )),
                    Some(summary) => Ok(Ok(summary.clone())),
                }
            }
            RecordBuffer::Transaction => Err(result_out_of_scope_error()),
        }
    }
}

#[derive(Debug)]
enum TxFailState {
    Failed,
    Passed,
}

#[derive(Debug)]
pub(crate) enum Command {
    AutoCommit(AutoCommit),
    BeginTransaction(BeginTransaction),
    TransactionRun(TransactionRun),
    CommitTransaction(CommitTransaction),
    RollbackTransaction(RollbackTransaction),
    CloseTransaction(CloseTransaction),
    ResultNext(ResultNext),
    ResultSingle(ResultSingle),
    ResultConsume(ResultConsume),
    Close,
}

#[derive(Debug)]
pub(crate) enum CommandResult {
    AutoCommit(AutoCommitResult),
    BeginTransaction(BeginTransactionResult),
    TransactionRun(TransactionRunResult),
    CommitTransaction(CommitTransactionResult),
    RollbackTransaction(RollbackTransactionResult),
    CloseTransaction(CloseTransactionResult),
    ResultNext(ResultNextResult),
    ResultSingle(ResultSingleResult),
    ResultConsume(ResultConsumeResult),
    Close(CloseResult),
}

impl Command {
    fn reply_error(&self, err: TestKitError, tx_res: &Sender<CommandResult>) {
        let msg = match self {
            Command::AutoCommit(_) => AutoCommitResult { result: Err(err) }.into(),
            Command::BeginTransaction(_) => BeginTransactionResult { result: Err(err) }.into(),
            Command::TransactionRun(_) => TransactionRunResult { result: Err(err) }.into(),
            Command::CommitTransaction(_) => CommitTransactionResult { result: Err(err) }.into(),
            Command::RollbackTransaction(_) => {
                RollbackTransactionResult { result: Err(err) }.into()
            }
            Command::CloseTransaction(_) => CloseTransactionResult { result: Err(err) }.into(),
            Command::ResultNext(_) => ResultNextResult { result: Err(err) }.into(),
            Command::ResultSingle(_) => ResultSingleResult { result: Err(err) }.into(),
            Command::ResultConsume(_) => ResultConsumeResult { result: Err(err) }.into(),
            Command::Close => CloseResult { result: Err(err) }.into(),
        };
        tx_res.send(msg).unwrap();
    }
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
    pub(crate) result: Result<(BackendId, RecordKeys), TestKitError>,
}

impl From<AutoCommitResult> for CommandResult {
    fn from(r: AutoCommitResult) -> Self {
        CommandResult::AutoCommit(r)
    }
}

#[derive(Debug)]
pub(crate) struct BeginTransaction {
    pub(crate) session_id: BackendId,
    pub(crate) tx_meta: Option<HashMap<String, ValueSend>>,
    pub(crate) timeout: Option<i64>,
}

impl From<BeginTransaction> for Command {
    fn from(c: BeginTransaction) -> Self {
        Command::BeginTransaction(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct BeginTransactionResult {
    pub(crate) result: Result<Result<BackendId, Neo4jError>, TestKitError>,
}

impl From<BeginTransactionResult> for CommandResult {
    fn from(r: BeginTransactionResult) -> Self {
        CommandResult::BeginTransaction(r)
    }
}

#[derive(Debug)]
pub(crate) struct TransactionRun {
    pub(crate) transaction_id: BackendId,
    pub(crate) query: String,
    pub(crate) params: Option<HashMap<String, ValueSend>>,
}

impl From<TransactionRun> for Command {
    fn from(c: TransactionRun) -> Self {
        Command::TransactionRun(c)
    }
}

impl TransactionRun {
    fn default_response(
        &self,
        tx_res: &Sender<CommandResult>,
        known_transactions: &HashMap<BackendId, TxFailState>,
    ) {
        let err = match known_transactions.get(&self.transaction_id) {
            Some(_) => transaction_out_of_scope_error(),
            None => TestKitError::backend_err("transaction not found"),
        };
        let msg = CommandResult::TransactionRun(TransactionRunResult { result: Err(err) });
        tx_res.send(msg).unwrap();
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct TransactionRunResult {
    pub(crate) result: Result<Result<(BackendId, RecordKeys), Neo4jError>, TestKitError>,
}

impl From<TransactionRunResult> for CommandResult {
    fn from(r: TransactionRunResult) -> Self {
        CommandResult::TransactionRun(r)
    }
}

#[derive(Debug)]
pub(crate) struct CommitTransaction {
    pub(crate) transaction_id: BackendId,
}

impl From<CommitTransaction> for Command {
    fn from(c: CommitTransaction) -> Self {
        Command::CommitTransaction(c)
    }
}

impl CommitTransaction {
    fn default_response(
        &self,
        tx_res: &Sender<CommandResult>,
        known_transactions: &HashMap<BackendId, TxFailState>,
    ) {
        let err = match known_transactions.get(&self.transaction_id) {
            Some(_) => transaction_out_of_scope_error(),
            None => TestKitError::backend_err("transaction not found"),
        };
        let msg = CommandResult::CommitTransaction(CommitTransactionResult { result: Err(err) });
        tx_res.send(msg).unwrap();
    }

    fn real_response(&self, result: Result<(), Neo4jError>, tx_res: &Sender<CommandResult>) {
        let msg = CommandResult::CommitTransaction(CommitTransactionResult { result: Ok(result) });
        tx_res.send(msg).unwrap();
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct CommitTransactionResult {
    pub(crate) result: Result<Result<(), Neo4jError>, TestKitError>,
}

impl From<CommitTransactionResult> for CommandResult {
    fn from(r: CommitTransactionResult) -> Self {
        CommandResult::CommitTransaction(r)
    }
}

#[derive(Debug)]
pub(crate) struct RollbackTransaction {
    pub(crate) transaction_id: BackendId,
}

impl From<RollbackTransaction> for Command {
    fn from(c: RollbackTransaction) -> Self {
        Command::RollbackTransaction(c)
    }
}

impl RollbackTransaction {
    fn default_response(
        &self,
        tx_res: &Sender<CommandResult>,
        known_transactions: &HashMap<BackendId, TxFailState>,
    ) {
        let result = match known_transactions.get(&self.transaction_id) {
            Some(TxFailState::Passed) => Err(transaction_out_of_scope_error()),
            Some(TxFailState::Failed) => Ok(Ok(())),
            None => Err(TestKitError::backend_err("transaction not found")),
        };
        let msg = CommandResult::RollbackTransaction(RollbackTransactionResult { result });
        tx_res.send(msg).unwrap();
    }

    fn real_response(&self, result: Result<(), Neo4jError>, tx_res: &Sender<CommandResult>) {
        let msg =
            CommandResult::RollbackTransaction(RollbackTransactionResult { result: Ok(result) });
        tx_res.send(msg).unwrap();
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct RollbackTransactionResult {
    pub(crate) result: Result<Result<(), Neo4jError>, TestKitError>,
}

impl From<RollbackTransactionResult> for CommandResult {
    fn from(r: RollbackTransactionResult) -> Self {
        CommandResult::RollbackTransaction(r)
    }
}

#[derive(Debug)]
pub(crate) struct CloseTransaction {
    pub(crate) transaction_id: BackendId,
}

impl From<CloseTransaction> for Command {
    fn from(c: CloseTransaction) -> Self {
        Command::CloseTransaction(c)
    }
}

impl CloseTransaction {
    fn default_response(
        &self,
        tx_res: &Sender<CommandResult>,
        known_transactions: &HashMap<BackendId, TxFailState>,
    ) {
        let result = match known_transactions.get(&self.transaction_id) {
            Some(_) => Ok(Ok(())),
            None => Err(TestKitError::backend_err("transaction not found")),
        };
        let msg = CommandResult::CloseTransaction(CloseTransactionResult { result });
        tx_res.send(msg).unwrap();
    }

    fn real_response(&self, result: Result<(), Neo4jError>, tx_res: &Sender<CommandResult>) {
        let msg = CommandResult::CloseTransaction(CloseTransactionResult { result: Ok(result) });
        tx_res.send(msg).unwrap();
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct CloseTransactionResult {
    pub(crate) result: Result<Result<(), Neo4jError>, TestKitError>,
}

impl From<CloseTransactionResult> for CommandResult {
    fn from(r: CloseTransactionResult) -> Self {
        CommandResult::CloseTransaction(r)
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
pub(crate) struct ResultSingle {
    pub(crate) result_id: BackendId,
}

impl From<ResultSingle> for Command {
    fn from(c: ResultSingle) -> Self {
        Command::ResultSingle(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct ResultSingleResult {
    pub(crate) result: Result<Result<Record, Neo4jError>, TestKitError>,
}

impl From<ResultSingleResult> for CommandResult {
    fn from(r: ResultSingleResult) -> Self {
        CommandResult::ResultSingle(r)
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

#[must_use]
#[derive(Debug)]
pub(crate) struct CloseResult {
    pub(crate) result: Result<(), TestKitError>,
}

impl From<CloseResult> for CommandResult {
    fn from(r: CloseResult) -> Self {
        CommandResult::Close(r)
    }
}

pub(crate) type RecordKeys = Vec<Arc<String>>;
