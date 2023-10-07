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

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use flume;
use flume::{Receiver, Sender};
use log::warn;

use crate::driver::{ConnectionConfig, Driver, DriverConfig, RoutingControl};
use crate::retry::ExponentialBackoff;
use crate::session::SessionConfig;
use crate::testkit_backend::session_holder::RetryableOutcome;

use super::backend_id::{BackendId, Generator};
use super::errors::TestKitError;
use super::session_holder::SessionHolder;
use super::session_holder::{
    AutoCommit, AutoCommitResult, BeginTransaction, BeginTransactionResult,
    CloseResult as CloseSessionResult, CloseTransaction, CloseTransactionResult, CommitTransaction,
    CommitTransactionResult, LastBookmarks, LastBookmarksResult, ResultConsume,
    ResultConsumeResult, ResultNext, ResultNextResult, ResultSingle, ResultSingleResult,
    RetryableNegative, RetryableNegativeResult, RetryablePositive, RetryablePositiveResult,
    RollbackTransaction, RollbackTransactionResult, TransactionFunction, TransactionFunctionResult,
    TransactionRun, TransactionRunResult,
};

#[derive(Debug)]
pub(super) struct DriverHolder {
    tx_req: Sender<Command>,
    rx_res: Receiver<CommandResult>,
    join_handle: Option<JoinHandle<()>>,
}

impl DriverHolder {
    pub(super) fn new(
        id: &BackendId,
        id_generator: Generator,
        connection_config: ConnectionConfig,
        config: DriverConfig,
        emulated_config: EmulatedDriverConfig,
    ) -> Self {
        let driver = Arc::new(Driver::new(connection_config, config));
        let emulated_config = Arc::new(emulated_config);
        let (tx_req, rx_req) = flume::unbounded();
        let (tx_res, rx_res) = flume::unbounded();
        // let session_ids = Arc::new(Mutex::new(HashSet::new()));
        // let result_ids = Arc::new(Mutex::new(HashSet::new()));
        let handle = {
            // let session_ids = Arc::clone(&session_ids);
            // let result_ids = Arc::clone(&result_ids);
            thread::Builder::new()
                .name(format!("d-{id}"))
                .spawn(move || {
                    let runner = DriverHolderRunner {
                        id_generator,
                        // session_ids,
                        // result_ids,
                        rx_req,
                        tx_res,
                        driver,
                        emulated_config,
                    };
                    runner.run();
                })
                .expect("Failed to spawn DriverHolderRunner thread")
        };
        Self {
            tx_req,
            rx_res,
            join_handle: Some(handle),
        }
    }

    pub(super) fn session(&self, args: NewSession) -> NewSessionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::NewSession(result) => result,
            res => panic!("expected CommandResult::NewSession, found {res:?}"),
        }
    }

    pub(super) fn session_close(&self, args: CloseSession) -> CloseSessionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::CloseSession(result) => result,
            res => panic!("expected CommandResult::CloseSession, found {res:?}"),
        }
    }

    pub(super) fn auto_commit(&self, args: AutoCommit) -> AutoCommitResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::AutoCommit(result) => result,
            res => panic!("expected CommandResult::AutoCommit, found {res:?}"),
        }
    }

    pub(super) fn transaction_function(
        &self,
        args: TransactionFunction,
    ) -> TransactionFunctionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::TransactionFunction(result) => result,
            res => panic!("expected CommandResult::TransactionFunction, found {res:?}"),
        }
    }

    pub(super) fn retryable_positive(&self, args: RetryablePositive) -> RetryablePositiveResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::RetryablePositive(result) => result,
            res => panic!("expected CommandResult::RetryablePositive, found {res:?}"),
        }
    }

    pub(super) fn retryable_negative(&self, args: RetryableNegative) -> RetryableNegativeResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::RetryableNegative(result) => result,
            res => panic!("expected CommandResult::RetryableNegative, found {res:?}"),
        }
    }

    pub(super) fn begin_transaction(&self, args: BeginTransaction) -> BeginTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::BeginTransaction(result) => result,
            res => panic!("expected CommandResult::BeginTransaction, found {res:?}"),
        }
    }

    pub(super) fn transaction_run(&self, args: TransactionRun) -> TransactionRunResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::TransactionRun(result) => result,
            res => panic!("expected CommandResult::TransactionRun, found {res:?}"),
        }
    }

    pub(super) fn commit_transaction(&self, args: CommitTransaction) -> CommitTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::CommitTransaction(result) => result,
            res => panic!("expected CommandResult::CommitTransaction, found {res:?}"),
        }
    }

    pub(super) fn rollback_transaction(
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

    pub(super) fn close_transaction(&self, args: CloseTransaction) -> CloseTransactionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::CloseTransaction(result) => result,
            res => panic!("expected CommandResult::CloseTransaction, found {res:?}"),
        }
    }

    pub(super) fn result_next(&self, args: ResultNext) -> ResultNextResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultNext(result) => result,
            res => panic!("expected CommandResult::ResultNext, found {res:?}"),
        }
    }

    pub(super) fn result_single(&self, args: ResultSingle) -> ResultSingleResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultSingle(result) => result,
            res => panic!("expected CommandResult::ResultSingle, found {res:?}"),
        }
    }

    pub(super) fn result_consume(&self, args: ResultConsume) -> ResultConsumeResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::ResultConsume(result) => result,
            res => panic!("expected CommandResult::ResultConsume, found {res:?}"),
        }
    }

    pub(super) fn last_bookmarks(&self, args: LastBookmarks) -> LastBookmarksResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::LastBookmarks(result) => result,
            res => panic!("expected CommandResult::LastBookmarks, found {res:?}"),
        }
    }

    pub(super) fn close(&mut self) -> CloseResult {
        let Some(handle) = self.join_handle.take() else {
            return CloseResult { result: Ok(()) };
        };
        self.tx_req.send(Command::Close).unwrap();
        let res = match self.rx_res.recv().unwrap() {
            CommandResult::Close(result) => result,
            res => panic!("expected CommandResult::Close, found {res:?}"),
        };
        handle.join().unwrap();
        res
    }
}

impl Drop for DriverHolder {
    fn drop(&mut self) {
        if let CloseResult { result: Err(e) } = self.close() {
            warn!("Ignored driver closure error on drop: {e:?}");
        }
    }
}

#[derive(Debug)]
struct DriverHolderRunner {
    id_generator: Generator,
    // session_ids: Arc<Mutex<HashSet<BackendId>>>,
    // result_ids: Arc<Mutex<HashSet<BackendId>>>,
    rx_req: Receiver<Command>,
    tx_res: Sender<CommandResult>,
    driver: Arc<Driver>,
    emulated_config: Arc<EmulatedDriverConfig>,
}

impl DriverHolderRunner {
    fn run(&self) {
        let mut sessions = HashMap::new();
        let mut result_id_to_session_id = HashMap::new();
        let mut tx_id_to_session_id = HashMap::new();
        loop {
            let res = match self.rx_req.recv().unwrap() {
                Command::NewSession(NewSession {
                    auto_commit_access_mode,
                    config,
                }) => {
                    let session_id = self.id_generator.next_id();
                    let session_holder = SessionHolder::new(
                        &session_id,
                        self.id_generator.clone(),
                        Arc::clone(&self.driver),
                        auto_commit_access_mode,
                        config,
                        Arc::clone(&self.emulated_config),
                    );
                    sessions.insert(session_id, session_holder);
                    Some(NewSessionResult { session_id }.into())
                }

                Command::AutoCommit(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(
                            AutoCommitResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let res = session_holder.auto_commit(args);
                    if let Ok((result_id, _)) = res.result {
                        result_id_to_session_id.insert(result_id, session_id);
                    }
                    Some(res.into())
                }

                Command::TransactionFunction(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(
                            TransactionFunctionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let res = session_holder.transaction_function(args);
                    if let Ok(RetryableOutcome::Retry(tx_id)) = res.result {
                        tx_id_to_session_id.insert(tx_id, session_id);
                    }
                    Some(res.into())
                }

                Command::RetryablePositive(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(
                            RetryablePositiveResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let res = session_holder.retryable_positive(args);
                    if let Ok(RetryableOutcome::Retry(tx_id)) = res.result {
                        tx_id_to_session_id.insert(tx_id, session_id);
                    }
                    Some(res.into())
                }

                Command::RetryableNegative(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(
                            RetryableNegativeResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let res = session_holder.retryable_negative(args);
                    if let Ok(RetryableOutcome::Retry(tx_id)) = res.result {
                        tx_id_to_session_id.insert(tx_id, session_id);
                    }
                    Some(res.into())
                }

                Command::BeginTransaction(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(
                            BeginTransactionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let res = session_holder.begin_transaction(args);
                    if let Ok(tx_id) = res.result {
                        tx_id_to_session_id.insert(tx_id, session_id);
                    }
                    Some(res.into())
                }

                Command::TransactionRun(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(
                            TransactionRunResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Transaction id {} not found in driver",
                                    &args.transaction_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.transaction_run(args);
                    if let Ok((result_id, _)) = res.result {
                        result_id_to_session_id.insert(result_id, *session_id);
                    }
                    Some(res.into())
                }

                Command::CommitTransaction(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(
                            CommitTransactionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Transaction id {} not found in driver",
                                    &args.transaction_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.commit_transaction(args);
                    Some(res.into())
                }

                Command::RollbackTransaction(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(
                            RollbackTransactionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Transaction id {} not found in driver",
                                    &args.transaction_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.rollback_transaction(args);
                    Some(res.into())
                }

                Command::CloseTransaction(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(
                            CloseTransactionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Transaction id {} not found in driver",
                                    &args.transaction_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.close_transaction(args);
                    Some(res.into())
                }

                Command::ResultNext(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(
                            ResultNextResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Result id {} not found in driver",
                                    &args.result_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_next(args).into())
                }

                Command::ResultSingle(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(
                            ResultSingleResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Result id {} not found in driver",
                                    &args.result_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_single(args).into())
                }

                Command::ResultConsume(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(
                            ResultConsumeResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Result id {} not found in driver",
                                    &args.result_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_consume(args).into())
                }

                Command::LastBookmarks(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(
                            BeginTransactionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    Some(session_holder.last_bookmarks(args).into())
                }

                Command::CloseSession(CloseSession { session_id }) => 'arm: {
                    let Some(mut session) = sessions.remove(&session_id) else {
                        break 'arm Some(
                            CloseSessionResult {
                                result: Err(TestKitError::backend_err(format!(
                                    "Session id {} not found in driver",
                                    session_id
                                ))),
                            }
                            .into(),
                        );
                    };
                    Some(session.close().into())
                }

                Command::Close => {
                    let result = sessions
                        .into_iter()
                        .try_for_each(|(_, mut session)| session.close().result);
                    let msg = CloseResult { result }.into();
                    self.tx_res.send(msg).unwrap();
                    return;
                }
            };
            if let Some(res) = res {
                self.tx_res.send(res).unwrap();
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    NewSession(NewSession),
    CloseSession(CloseSession),
    AutoCommit(AutoCommit),
    TransactionFunction(TransactionFunction),
    RetryablePositive(RetryablePositive),
    RetryableNegative(RetryableNegative),
    BeginTransaction(BeginTransaction),
    TransactionRun(TransactionRun),
    CommitTransaction(CommitTransaction),
    RollbackTransaction(RollbackTransaction),
    CloseTransaction(CloseTransaction),
    ResultNext(ResultNext),
    ResultSingle(ResultSingle),
    ResultConsume(ResultConsume),
    LastBookmarks(LastBookmarks),
    Close,
}

#[derive(Debug)]
enum CommandResult {
    NewSession(NewSessionResult),
    CloseSession(CloseSessionResult),
    AutoCommit(AutoCommitResult),
    TransactionFunction(TransactionFunctionResult),
    RetryablePositive(RetryablePositiveResult),
    RetryableNegative(RetryableNegativeResult),
    BeginTransaction(BeginTransactionResult),
    TransactionRun(TransactionRunResult),
    CommitTransaction(CommitTransactionResult),
    RollbackTransaction(RollbackTransactionResult),
    CloseTransaction(CloseTransactionResult),
    ResultNext(ResultNextResult),
    ResultSingle(ResultSingleResult),
    ResultConsume(ResultConsumeResult),
    LastBookmarks(LastBookmarksResult),
    Close(CloseResult),
}

#[derive(Debug)]
pub(super) struct NewSession {
    pub(super) auto_commit_access_mode: RoutingControl,
    pub(super) config: SessionConfig,
}

impl From<NewSession> for Command {
    fn from(c: NewSession) -> Self {
        Command::NewSession(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(super) struct NewSessionResult {
    pub(super) session_id: BackendId,
}

impl From<NewSessionResult> for CommandResult {
    fn from(r: NewSessionResult) -> Self {
        CommandResult::NewSession(r)
    }
}

#[derive(Debug)]
pub(super) struct CloseSession {
    pub(super) session_id: BackendId,
}

impl From<CloseSession> for Command {
    fn from(c: CloseSession) -> Self {
        Command::CloseSession(c)
    }
}

impl From<CloseSessionResult> for CommandResult {
    fn from(r: CloseSessionResult) -> Self {
        CommandResult::CloseSession(r)
    }
}

impl From<AutoCommit> for Command {
    fn from(c: AutoCommit) -> Self {
        Command::AutoCommit(c)
    }
}

impl From<AutoCommitResult> for CommandResult {
    fn from(r: AutoCommitResult) -> Self {
        CommandResult::AutoCommit(r)
    }
}

impl From<TransactionFunction> for Command {
    fn from(c: TransactionFunction) -> Self {
        Command::TransactionFunction(c)
    }
}

impl From<TransactionFunctionResult> for CommandResult {
    fn from(r: TransactionFunctionResult) -> Self {
        CommandResult::TransactionFunction(r)
    }
}

impl From<RetryablePositive> for Command {
    fn from(c: RetryablePositive) -> Self {
        Command::RetryablePositive(c)
    }
}

impl From<RetryablePositiveResult> for CommandResult {
    fn from(r: RetryablePositiveResult) -> Self {
        CommandResult::RetryablePositive(r)
    }
}

impl From<RetryableNegative> for Command {
    fn from(c: RetryableNegative) -> Self {
        Command::RetryableNegative(c)
    }
}

impl From<RetryableNegativeResult> for CommandResult {
    fn from(r: RetryableNegativeResult) -> Self {
        CommandResult::RetryableNegative(r)
    }
}

impl From<BeginTransaction> for Command {
    fn from(c: BeginTransaction) -> Self {
        Command::BeginTransaction(c)
    }
}

impl From<BeginTransactionResult> for CommandResult {
    fn from(r: BeginTransactionResult) -> Self {
        CommandResult::BeginTransaction(r)
    }
}

impl From<TransactionRun> for Command {
    fn from(c: TransactionRun) -> Self {
        Command::TransactionRun(c)
    }
}

impl From<TransactionRunResult> for CommandResult {
    fn from(r: TransactionRunResult) -> Self {
        CommandResult::TransactionRun(r)
    }
}

impl From<CommitTransaction> for Command {
    fn from(c: CommitTransaction) -> Self {
        Command::CommitTransaction(c)
    }
}

impl From<CommitTransactionResult> for CommandResult {
    fn from(r: CommitTransactionResult) -> Self {
        CommandResult::CommitTransaction(r)
    }
}

impl From<RollbackTransaction> for Command {
    fn from(c: RollbackTransaction) -> Self {
        Command::RollbackTransaction(c)
    }
}

impl From<RollbackTransactionResult> for CommandResult {
    fn from(r: RollbackTransactionResult) -> Self {
        CommandResult::RollbackTransaction(r)
    }
}

impl From<CloseTransaction> for Command {
    fn from(c: CloseTransaction) -> Self {
        Command::CloseTransaction(c)
    }
}

impl From<CloseTransactionResult> for CommandResult {
    fn from(r: CloseTransactionResult) -> Self {
        CommandResult::CloseTransaction(r)
    }
}

impl From<ResultNext> for Command {
    fn from(c: ResultNext) -> Self {
        Command::ResultNext(c)
    }
}

impl From<ResultNextResult> for CommandResult {
    fn from(r: ResultNextResult) -> Self {
        CommandResult::ResultNext(r)
    }
}

impl From<ResultSingle> for Command {
    fn from(c: ResultSingle) -> Self {
        Command::ResultSingle(c)
    }
}

impl From<ResultSingleResult> for CommandResult {
    fn from(r: ResultSingleResult) -> Self {
        CommandResult::ResultSingle(r)
    }
}

impl From<ResultConsume> for Command {
    fn from(c: ResultConsume) -> Self {
        Command::ResultConsume(c)
    }
}

impl From<LastBookmarks> for Command {
    fn from(c: LastBookmarks) -> Self {
        Command::LastBookmarks(c)
    }
}

impl From<LastBookmarksResult> for CommandResult {
    fn from(r: LastBookmarksResult) -> Self {
        CommandResult::LastBookmarks(r)
    }
}

impl From<ResultConsumeResult> for CommandResult {
    fn from(r: ResultConsumeResult) -> Self {
        CommandResult::ResultConsume(r)
    }
}

#[must_use]
#[derive(Debug)]
pub(super) struct CloseResult {
    pub(super) result: Result<(), TestKitError>,
}

impl From<CloseResult> for CommandResult {
    fn from(r: CloseResult) -> Self {
        CommandResult::Close(r)
    }
}

#[derive(Debug, Default)]
pub(super) struct EmulatedDriverConfig {
    retry_policy: ExponentialBackoff,
}

impl EmulatedDriverConfig {
    pub(super) fn with_max_retry_time(mut self, max_retry_time: Duration) -> Self {
        self.retry_policy = self.retry_policy.with_max_retry_time(max_retry_time);
        self
    }

    pub(super) fn retry_policy(&self) -> ExponentialBackoff {
        self.retry_policy
    }
}
