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

use crate::driver::{ConnectionConfig, Driver, DriverConfig, RoutingControl};
use crate::session::SessionConfig;

use super::backend_id::{BackendId, Generator};
use super::errors::TestKitError;
use super::session_holder::SessionHolder;
use super::session_holder::{
    AutoCommit, AutoCommitResult, ResultConsume, ResultConsumeResult, ResultNext, ResultNextResult,
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

impl Drop for DriverHolder {
    fn drop(&mut self) {
        self.close()
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

                Command::ResultNext(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(ResultNextResult{result: Err(TestKitError::backend_err(format!("Result id {} not found in driver", &args.result_id)))}.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_next(args).into())
                }

                Command::ResultConsume(args) => 'arm: {
                    let Some(session_id) = result_id_to_session_id.get(&args.result_id) else {
                        break 'arm Some(ResultNextResult { result: Err(TestKitError::backend_err(format!("Result id {} not found in driver", &args.result_id))) }.into());
                    };
                    let session_holder = sessions.get(session_id).unwrap();
                    Some(session_holder.result_consume(args).into())
                }

                Command::CloseSession(CloseSession { session_id }) => 'arm: {
                    let Some(_) = sessions.remove(&session_id) else {
                        break 'arm Some(AutoCommitResult{result: Err(TestKitError::backend_err(format!("Session id {} not found in driver", session_id)))}.into());
                    };
                    sessions.remove(&session_id);
                    Some(CloseSessionResult { result: Ok(()) }.into())
                }

                Command::Close => {
                    break;
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
    ResultNext(ResultNext),
    ResultConsume(ResultConsume),
    Close,
}

#[derive(Debug)]
enum CommandResult {
    NewSession(NewSessionResult),
    CloseSession(CloseSessionResult),
    AutoCommit(AutoCommitResult),
    ResultNext(ResultNextResult),
    ResultConsume(ResultConsumeResult),
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

#[must_use]
#[derive(Debug)]
pub(crate) struct CloseSessionResult {
    pub(crate) result: Result<(), TestKitError>,
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
