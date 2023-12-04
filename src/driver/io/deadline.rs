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

use log::warn;
use std::fmt::Debug;
use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::time::Instant;
use crate::{Neo4jError, Result};

pub(crate) fn rewrite_timeout<R, F: FnOnce() -> Result<R>>(f: F) -> Result<R> {
    match f() {
        Err(Neo4jError::Disconnect {
            message,
            source: Some(io_err),
            during_commit,
        }) => {
            let kind = io_err.kind();
            match kind {
                ErrorKind::WouldBlock | ErrorKind::TimedOut => {
                    assert!(
                        !during_commit,
                        "tried to rewrite io error to timeout error during commit"
                    );
                    Err(Neo4jError::Timeout { message })
                }
                _ => Err(Neo4jError::Disconnect {
                    message,
                    source: Some(io_err),
                    during_commit,
                }),
            }
        }
        v => v,
    }
}

fn wrap_set_timeout_error<T>(e: Result<T>) -> Result<T> {
    match e {
        Ok(t) => Ok(t),
        Err(e) => Err(Neo4jError::InvalidConfig {
            message: format!("failed to configure timeout: {e}"),
        }),
    }
}

fn wrap_get_timeout_error<T>(e: Result<T>) -> Result<T> {
    match e {
        Ok(t) => Ok(t),
        Err(e) => Err(Neo4jError::InvalidConfig {
            message: format!("failed to read configured timeout: {e}"),
        }),
    }
}

enum ReaderErrorDuring {
    GetTimeout,
    SetTimeout,
    IO,
}

pub(crate) struct DeadlineIO<'tcp, S> {
    stream: S,
    deadline: Option<Instant>,
    socket: Option<&'tcp TcpStream>,
    error_during: Option<ReaderErrorDuring>,
}

impl<'tcp, S: Read + Write> DeadlineIO<'tcp, S> {
    pub(crate) fn new(
        stream: S,
        deadline: Option<Instant>,
        socket: Option<&'tcp TcpStream>,
    ) -> Self {
        Self {
            stream,
            deadline,
            socket,
            error_during: None,
        }
    }

    fn wrap_io_error<T>(&mut self, res: io::Result<T>, during: ReaderErrorDuring) -> io::Result<T> {
        if res.is_err() {
            self.error_during = Some(during);
        }
        res
    }

    fn with_deadline<T: Debug, F: FnOnce(&mut Self) -> io::Result<T>>(
        &mut self,
        work: F,
    ) -> io::Result<T> {
        let Some(deadline) = self.deadline else {
            let res = work(self);
            return self.wrap_io_error(res, ReaderErrorDuring::IO);
        };
        let Some(socket) = self.socket else {
            let res = work(self);
            return self.wrap_io_error(res, ReaderErrorDuring::IO);
        };
        let old_timeout =
            self.wrap_io_error(get_socket_timeout(socket), ReaderErrorDuring::GetTimeout)?;
        let timeout = match deadline.checked_duration_since(Instant::now()) {
            // deadline in the past
            // => we set a tiny timeout to trigger a timeout error on pretty much any blocking
            None => Duration::from_nanos(1),
            // deadline in the future
            Some(timeout) => timeout,
        };
        self.wrap_io_error(
            set_socket_timeout(socket, Some(timeout)),
            ReaderErrorDuring::SetTimeout,
        )?;
        let res = work(self);
        let res = self.wrap_io_error(res, ReaderErrorDuring::IO);
        if let Err(err) = set_socket_timeout(socket, old_timeout) {
            warn!("failed to restore timeout: {}", err)
        }
        res
    }

    pub(crate) fn rewrite_error<T>(&self, res: Result<T>) -> Result<T> {
        if res.is_ok() {
            return res;
        }
        match self.error_during {
            Some(ReaderErrorDuring::GetTimeout) => wrap_get_timeout_error(res),
            Some(ReaderErrorDuring::SetTimeout) => wrap_set_timeout_error(res),
            Some(ReaderErrorDuring::IO) | None => res,
        }
    }
}

fn get_socket_timeout(socket: &TcpStream) -> io::Result<Option<Duration>> {
    socket.read_timeout()
}

fn set_socket_timeout(socket: &TcpStream, timeout: Option<Duration>) -> io::Result<()> {
    socket.set_read_timeout(timeout)?;
    socket.set_write_timeout(timeout)
}

impl<'tcp, S> Debug for DeadlineIO<'tcp, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadlineIO")
            .field("deadline", &self.deadline)
            .field("socket", &self.socket)
            .finish()
    }
}

impl<'tcp, S: Read + Write> Read for DeadlineIO<'tcp, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.with_deadline(|self_| self_.stream.read(buf))
    }
}

impl<'tcp, S: Read + Write> Write for DeadlineIO<'tcp, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.with_deadline(|self_| self_.stream.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.with_deadline(|self_| self_.stream.flush())
    }
}
