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

use log::debug;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

mod backend_id;
mod cypher_value;
mod driver_holder;
mod errors;
mod requests;
mod responses;
mod session_holder;

use crate::testkit_backend::responses::Response;
pub(crate) use backend_id::BackendId;
use backend_id::Generator;
use driver_holder::DriverHolder;
use errors::TestKitError;
use requests::Request;

const ADDRESS: &str = "0.0.0.0:9876";

type DynError = Box<dyn Error>;
type TestKitResult = Result<(), TestKitError>;

pub fn main() {
    start_server()
}

fn start_server() {
    let listener = TcpListener::bind(ADDRESS).unwrap();
    println!("Listening on {}", ADDRESS);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let err = handle_stream(stream);
                println!("TestKit disconnected {err:?}")
            }
            Err(err) => {
                println!("Connection failed {:?}", err);
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
    println!("TestKit connected {remote}");
    let reader = BufReader::new(match stream.try_clone() {
        Ok(ok) => ok,
        Err(err) => return err.into(),
    });
    let writer = BufWriter::new(stream);
    let mut backend = Backend::new(reader, writer);
    loop {
        if let Err(err) = backend.handle_request() {
            return err;
        }
    }
}

#[derive(Debug)]
pub(crate) struct Backend {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,

    id_generator: Generator,
    drivers: HashMap<BackendId, Option<DriverHolder>>,
    session_id_to_driver_id: HashMap<BackendId, Option<BackendId>>,
    result_id_to_driver_id: HashMap<BackendId, BackendId>,
    tx_id_to_driver_id: HashMap<BackendId, BackendId>,
}

impl Backend {
    fn new(reader: BufReader<TcpStream>, writer: BufWriter<TcpStream>) -> Self {
        Self {
            reader,
            writer,
            id_generator: Generator::new(),
            drivers: Default::default(),
            session_id_to_driver_id: Default::default(),
            result_id_to_driver_id: Default::default(),
            tx_id_to_driver_id: Default::default(),
        }
    }

    fn handle_request(&mut self) -> Result<(), DynError> {
        let mut in_request = false;
        let mut request = String::new();
        loop {
            let mut line = String::new();
            let read = self.reader.read_line(&mut line)?;
            if read == 0 {
                return Err(TestKitError::FatalError {
                    error: String::from("end of stream"),
                }
                .into());
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
                        }
                        .into());
                    }
                    in_request = true;
                }
                "#request end" => {
                    if !in_request {
                        return Err(TestKitError::FatalError {
                            error: String::from(
                                "received '#request end' while not processing a request",
                            ),
                        }
                        .into());
                    }
                    self.process_request(request)?;
                    break;
                }
                _ => {
                    if in_request {
                        request += &line;
                    }
                }
            }
        }
        Ok(())
    }

    fn process_request(&mut self, request: String) -> TestKitResult {
        debug!("<<< {request}");
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

    pub(crate) fn send<S: Serialize>(&mut self, message: &S) -> TestKitResult {
        let data = TestKitError::wrap_fatal(serde_json::to_string(message))?;
        debug!(">>> {data}");
        TestKitError::wrap_fatal(self.writer.write_all(b"#response begin\n"))?;
        TestKitError::wrap_fatal(self.writer.write_all(data.as_bytes()))?;
        TestKitError::wrap_fatal(self.writer.write_all(b"\n#response end\n"))?;
        TestKitError::wrap_fatal(self.writer.flush())?;
        Ok(())
    }

    pub(crate) fn send_err(&mut self, err: TestKitError) -> TestKitResult {
        let response = Response::try_from_testkit_error(err, &self.id_generator)?;
        self.send(&response)
    }

    pub(crate) fn next_id(&mut self) -> BackendId {
        self.id_generator.next_id()
    }
}
