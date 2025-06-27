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

mod auth;
mod backend_id;
mod bookmarks;
mod cypher_value;
mod driver_holder;
mod errors;
mod requests;
mod resolver;
mod responses;
mod session_holder;

use std::collections::HashMap;
use std::error::Error;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::panic;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::{debug, error, info, warn};
use serde::Serialize;

use neo4j::bookmarks::BookmarkManager;
use neo4j::driver::auth::AuthManager;

use super::logging;
use backend_id::BackendId;
use backend_id::Generator;
use driver_holder::DriverHolder;
use errors::TestKitError;
use requests::Request;
use responses::Response;
use session_holder::SummaryWithQuery;

const ADDRESS: &str = "0.0.0.0:9876";

type DynError = Box<dyn Error>;
type TestKitResultT<T> = Result<T, TestKitError>;
type TestKitResult = TestKitResultT<()>;

pub(super) fn start_server() {
    let listener = TcpListener::bind(ADDRESS).unwrap();
    println!("Listening on {ADDRESS}");
    for stream in listener.incoming() {
        logging::clear_log();
        match stream {
            Ok(stream) => {
                let res = panic::catch_unwind(|| handle_stream(stream));
                match res {
                    Ok(res) => info!("TestKit disconnected {res:?}"),
                    Err(err) => {
                        error!("TestKit panicked {err:?}");
                    }
                }
            }
            Err(err) => {
                warn!("Connection failed {:?}", err);
            }
        }
    }
}

fn handle_stream(stream: TcpStream) -> DynError {
    let remote = stream.peer_addr();
    let remote = match remote {
        Ok(ok) => ok,
        Err(err) => return err.into(),
    };
    info!("TestKit connected {remote}");
    let reader = BufReader::new(match stream.try_clone() {
        Ok(ok) => ok,
        Err(err) => return err.into(),
    });
    let writer = BufWriter::new(stream);
    let backend = Backend::new(reader, writer);
    loop {
        if let Err(err) = backend.handle_request() {
            cleanup();
            return err;
        }
    }
}

fn cleanup() {
    // ignore failure: we don't care if the time wasn't frozen
    let _ = neo4j::time::unfreeze_time();
}

#[derive(Debug, Clone)]
struct Backend {
    io: Arc<AtomicRefCell<BackendIo>>,
    data: Arc<AtomicRefCell<BackendData>>,
    id_generator: Generator,
}

#[derive(Debug)]
struct BackendIo {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

#[derive(Debug, Default)]
struct BackendData {
    drivers: HashMap<BackendId, Option<DriverHolder>>,
    summaries: HashMap<BackendId, SummaryWithQuery>,
    session_id_to_driver_id: HashMap<BackendId, Option<BackendId>>,
    result_id_to_driver_id: HashMap<BackendId, BackendId>,
    tx_id_to_driver_id: HashMap<BackendId, BackendId>,
    auth_managers: HashMap<BackendId, Arc<dyn AuthManager>>,
    bookmark_managers: HashMap<BackendId, Arc<dyn BookmarkManager>>,
}

impl Backend {
    fn new(reader: BufReader<TcpStream>, writer: BufWriter<TcpStream>) -> Self {
        let io = BackendIo { reader, writer };
        let data = BackendData::default();
        Self {
            io: Arc::new(AtomicRefCell::new(io)),
            data: Arc::new(AtomicRefCell::new(data)),
            id_generator: Generator::new(),
        }
    }
}

impl Backend {
    fn handle_request(&self) -> Result<(), DynError> {
        let request = {
            let mut io = self.io.borrow_mut();
            io.read_request()?
        };
        self.process_request(request)?;
        Ok(())
    }

    fn process_request(&self, request: String) -> TestKitResult {
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return self.send_err(TestKitError::from(err)),
        };
        let res = request.handle(self);
        if let Err(e) = res {
            if matches!(e, TestKitError::FatalError { .. }) {
                return Err(e);
            }
            self.send_err(e)?;
        }
        Ok(())
    }

    fn send<S: Serialize>(&self, message: &S) -> TestKitResult {
        self.io.borrow_mut().send(message)
    }

    fn send_err(&self, err: TestKitError) -> TestKitResult {
        self.io.borrow_mut().send_err(err, &self.id_generator)
    }

    fn next_id(&self) -> BackendId {
        self.id_generator.next_id()
    }
}

impl BackendIo {
    fn read_request(&mut self) -> TestKitResultT<String> {
        let mut in_request = false;
        let mut request = String::new();
        loop {
            let mut line = String::new();
            let read = self
                .reader
                .read_line(&mut line)
                .map_err(|e| TestKitError::FatalError {
                    error: format!("{e:?}"),
                })?;
            if read == 0 {
                return Err(TestKitError::FatalError {
                    error: String::from("end of stream"),
                });
            }
            while line.ends_with(char::is_whitespace) {
                line.pop();
            }
            match line.as_str() {
                "#request begin" => {
                    if in_request {
                        return Err(TestKitError::FatalError {
                            error: String::from(
                                "received '#request begin' while processing a request",
                            ),
                        });
                    }
                    in_request = true;
                }
                "#request end" => {
                    if !in_request {
                        return Err(TestKitError::FatalError {
                            error: String::from(
                                "received '#request end' while not processing a request",
                            ),
                        });
                    }
                    debug!("<<< {request}");
                    return Ok(request);
                }
                _ => {
                    if in_request {
                        request += &line;
                    }
                }
            }
        }
    }

    fn send_logs(&mut self) -> TestKitResult {
        let logs = logging::take_log();
        TestKitError::wrap_fatal(self.writer.write_all(&logs))?;
        TestKitError::wrap_fatal(self.writer.flush())?;
        Ok(())
    }

    fn send<S: Serialize>(&mut self, message: &S) -> TestKitResult {
        self.send_logs()?;
        let data = TestKitError::wrap_fatal(serde_json::to_string(message))?;
        debug!(">>> {data}");
        TestKitError::wrap_fatal(self.writer.write_all(b"#response begin\n"))?;
        TestKitError::wrap_fatal(self.writer.write_all(data.as_bytes()))?;
        TestKitError::wrap_fatal(self.writer.write_all(b"\n#response end\n"))?;
        TestKitError::wrap_fatal(self.writer.flush())?;
        Ok(())
    }

    fn send_err(&mut self, err: TestKitError, id_generator: &Generator) -> TestKitResult {
        self.send_logs()?;
        let response = Response::try_from_testkit_error(err, id_generator)?;
        self.send(&response)
    }
}

impl Drop for BackendData {
    fn drop(&mut self) {
        for (_, driver) in self.drivers.drain() {
            driver.map(|mut d| _ = d.close());
        }
    }
}
