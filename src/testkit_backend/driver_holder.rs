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

use flume;
use flume::{Receiver, Sender};
use log::warn;

use crate::driver::{ConnectionConfig, Driver, DriverConfig, RoutingControl};
use crate::session::SessionConfig;

use super::backend_id::{BackendId, Generator};
use super::errors::TestKitError;
use super::session_holder::SessionHolder;
use super::session_holder::{
    AutoCommit, AutoCommitResult, BeginTransaction, BeginTransactionResult,
    CloseResult as CloseSessionResult, CloseTransaction, CloseTransactionResult, CommitTransaction,
    CommitTransactionResult, ResultConsume, ResultConsumeResult, ResultNext, ResultNextResult,
    ResultSingle, ResultSingleResult, RollbackTransaction, RollbackTransactionResult,
    TransactionRun, TransactionRunResult,
};

#[derive(Debug)]
pub(crate) struct DriverHolder {
    tx_req: Sender<Command>,
    rx_res: Receiver<CommandResult>,
    join_handle: Option<JoinHandle<()>>,
}

impl DriverHolder {
    pub(crate) fn new(
        id_generator: Generator,
        connection_config: ConnectionConfig,
        config: DriverConfig,
    ) -> Self {
        let driver = Arc::new(Driver::new(connection_config, config));
        let (tx_req, rx_req) = flume::unbounded();
        let (tx_res, rx_res) = flume::unbounded();
        // let session_ids = Arc::new(Mutex::new(HashSet::new()));
        // let result_ids = Arc::new(Mutex::new(HashSet::new()));
        let handle = {
            // let session_ids = Arc::clone(&session_ids);
            // let result_ids = Arc::clone(&result_ids);
            thread::spawn(move || {
                let runner = DriverHolderRunner {
                    id_generator,
                    // session_ids,
                    // result_ids,
                    rx_req,
                    tx_res,
                    driver,
                };
                runner.run();
            })
        };
        Self {
            tx_req,
            rx_res,
            join_handle: Some(handle),
        }
    }

    pub(crate) fn session(&self, args: NewSession) -> NewSessionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::NewSession(result) => result,
            res => panic!("expected CommandResult::NewSession, found {:?}", res),
        }
    }

    pub(crate) fn session_close(&self, args: CloseSession) -> CloseSessionResult {
        self.tx_req.send(args.into()).unwrap();
        match self.rx_res.recv().unwrap() {
            CommandResult::CloseSession(result) => result,
            res => panic!("expected CommandResult::CloseSession, found {:?}", res),
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
                    let session_holder = SessionHolder::new(
                        self.id_generator.clone(),
                        Arc::clone(&self.driver),
                        auto_commit_access_mode,
                        config,
                    );
                    let session_id = self.id_generator.next_id();
                    sessions.insert(session_id, session_holder);
                    Some(NewSessionResult { session_id }.into())
                }

                Command::AutoCommit(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(AutoCommitResult{result: Err(TestKitError::backend_err(format!("Session id {} not found in driver", session_id)))}.into());
                    };
                    let res = session_holder.auto_commit(args);
                    if let Ok((result_id, _)) = res.result {
                        result_id_to_session_id.insert(result_id, session_id);
                    }
                    Some(res.into())
                }

                Command::BeginTransaction(args) => 'arm: {
                    let session_id = args.session_id;
                    let Some(session_holder) = sessions.get(&args.session_id) else {
                        break 'arm Some(BeginTransactionResult{result: Err(TestKitError::backend_err(format!("Session id {} not found in driver", session_id)))}.into());
                    };
                    let res = session_holder.begin_transaction(args);
                    if let Ok(Ok(tx_id)) = res.result {
                        tx_id_to_session_id.insert(tx_id, session_id);
                    }
                    Some(res.into())
                }

                Command::TransactionRun(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(TransactionRunResult{result: Err(TestKitError::backend_err(format!("Transaction id {} not found in driver", &args.transaction_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.transaction_run(args);
                    if let Ok(Ok((result_id, _))) = res.result {
                        result_id_to_session_id.insert(result_id, *session_id);
                    }
                    Some(res.into())
                }

                Command::CommitTransaction(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(CommitTransactionResult{result: Err(TestKitError::backend_err(format!("Transaction id {} not found in driver", &args.transaction_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.commit_transaction(args);
                    Some(res.into())
                }

                Command::RollbackTransaction(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(RollbackTransactionResult{result: Err(TestKitError::backend_err(format!("Transaction id {} not found in driver", &args.transaction_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.rollback_transaction(args);
                    Some(res.into())
                }

                Command::CloseTransaction(args) => 'arm: {
                    let Some(session_id) = tx_id_to_session_id.get(&args.transaction_id) else {
                        break 'arm Some(CloseTransactionResult{result: Err(TestKitError::backend_err(format!("Transaction id {} not found in driver", &args.transaction_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    let res = session_holder.close_transaction(args);
                    Some(res.into())
                }

                Command::ResultNext(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(ResultNextResult{result: Err(TestKitError::backend_err(format!("Result id {} not found in driver", &args.result_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_next(args).into())
                }

                Command::ResultSingle(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(ResultSingleResult{result: Err(TestKitError::backend_err(format!("Result id {} not found in driver", &args.result_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_single(args).into())
                }

                Command::ResultConsume(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(ResultConsumeResult{ result: Err(TestKitError::backend_err(format!("Result id {} not found in driver", &args.result_id))) }.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_consume(args).into())
                }

                Command::CloseSession(CloseSession { session_id }) => 'arm: {
                    let Some(mut session) = sessions.remove(&session_id) else {
                        break 'arm Some(CloseSessionResult{result: Err(TestKitError::backend_err(format!("Session id {} not found in driver", session_id)))}.into());
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
enum CommandResult {
    NewSession(NewSessionResult),
    CloseSession(CloseSessionResult),
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

#[derive(Debug)]
pub(crate) struct NewSession {
    pub(crate) auto_commit_access_mode: RoutingControl,
    pub(crate) config: SessionConfig,
}

impl From<NewSession> for Command {
    fn from(c: NewSession) -> Self {
        Command::NewSession(c)
    }
}

#[must_use]
#[derive(Debug)]
pub(crate) struct NewSessionResult {
    pub(crate) session_id: BackendId,
}

impl From<NewSessionResult> for CommandResult {
    fn from(r: NewSessionResult) -> Self {
        CommandResult::NewSession(r)
    }
}

#[derive(Debug)]
pub(crate) struct CloseSession {
    pub(crate) session_id: BackendId,
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
