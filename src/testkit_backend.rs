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

use std::error::Error;
use std::io::{BufRead, BufReader, BufWriter};
use std::net::{TcpListener, TcpStream};

mod errors;
mod requests;
mod responses;

use errors::{EndOfStream, TestKitProtocolError, TestKitWrappableError};
use requests::Request;

const ADDRESS: &str = "0.0.0.0:9876";

type DynError = Box<dyn Error>;
type MaybeDynError = Result<(), DynError>;

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
    let mut backend = Backend { reader, writer };
    loop {
        if let Err(err) = backend.handle_request() {
            return err;
        }
    }
}

pub(crate) struct Backend {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl Backend {
    fn handle_request(&mut self) -> MaybeDynError {
        let mut in_request = false;
        let mut request = String::new();
        loop {
            let mut line = String::new();
            let read = self.reader.read_line(&mut line)?;
            if read == 0 {
                return Err(EndOfStream {}.into());
            }
            while line.ends_with(char::is_whitespace) {
                line.pop();
            }
            match line.as_str() {
                "#request begin" => {
                    if in_request {
                        return self.wrap_backend_error(Err(TestKitProtocolError::new(
                            "received '#request begin' while processing a request",
                        )));
                    }
                    in_request = true;
                }
                "#request end" => {
                    if !in_request {
                        return self.wrap_backend_error(Err(TestKitProtocolError::new(
                            "received '#request end' while not processing a request",
                        )));
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

    fn process_request(&mut self, request: String) -> MaybeDynError {
        println!("<<< {request}");
        let request: Request = self.wrap_backend_error(serde_json::from_str(&request))?;
        request.handle(self)
    }

    pub(crate) fn wrap_backend_error<R, E: TestKitWrappableError + 'static>(
        &mut self,
        res: Result<R, E>,
    ) -> Result<R, DynError> {
        match res {
            Ok(ok) => Ok(ok),
            Err(err) => {
                err.wrap().send(self)?;
                Err(Box::new(err))
            }
        }
    }
}
