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

use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};

mod errors;
mod requests;
mod responses;

use crate::driver::Driver;
use errors::TestKitError;
use requests::Request;

const ADDRESS: &str = "0.0.0.0:9876";

type DynError = Box<dyn Error>;
type TestKitResult = Result<(), TestKitError>;
type BackendId = usize;

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
            return err.into();
        }
    }
}

pub(crate) struct Backend {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,

    next_id: BackendId,
    drivers: HashMap<BackendId, Driver>,
}

impl Backend {
    fn new(reader: BufReader<TcpStream>, writer: BufWriter<TcpStream>) -> Self {
        Self {
            reader,
            writer,
            next_id: Default::default(),
            drivers: Default::default(),
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
        println!("<<< {request}");
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return self.send(&TestKitError::from(err)),
        };
        let res = request.handle(self);
        if let Err(e) = res {
            if matches!(e, TestKitError::FatalError { .. }) {
                return Err(e);
            }
            TestKitError::wrap_fatal(self.send(&e))?;
        }
        Ok(())
    }

    pub(crate) fn send<S: Serialize>(&mut self, message: &S) -> TestKitResult {
        let data = TestKitError::wrap_fatal(serde_json::to_string(message))?;
        println!(">>> {data}");
        TestKitError::wrap_fatal(self.writer.write_all(b"#response begin\n"))?;
        TestKitError::wrap_fatal(self.writer.write_all(data.as_bytes()))?;
        TestKitError::wrap_fatal(self.writer.write_all(b"\n#response end\n"))?;
        TestKitError::wrap_fatal(self.writer.flush())?;
        Ok(())
    }

    pub(crate) fn next_id(&mut self) -> BackendId {
        let res = self.next_id;
        self.next_id += 1;
        return res;
    }
}
