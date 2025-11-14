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

use std::cmp;
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use log::warn;

use crate::error_::{Neo4jError, Result};
use crate::time::Instant;

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

enum DeadlineErrorDuring {
    GetTimeout,
    SetTimeout,
    IO,
}

pub(crate) struct DeadlineIO<'tcp, S> {
    stream: S,
    deadline: Option<Instant>,
    socket: Option<&'tcp TcpStream>,
    error_during: Option<DeadlineErrorDuring>,
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

    fn wrap_io_error<T>(
        &mut self,
        res: io::Result<T>,
        during: DeadlineErrorDuring,
    ) -> io::Result<T> {
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
            return self.wrap_io_error(res, DeadlineErrorDuring::IO);
        };
        let Some(socket) = self.socket else {
            let res = work(self);
            return self.wrap_io_error(res, DeadlineErrorDuring::IO);
        };
        let old_timeouts =
            self.wrap_io_error(get_socket_timeout(socket), DeadlineErrorDuring::GetTimeout)?;
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| {
                // deadline in the past
                // => we set a tiny timeout to trigger a timeout error on pretty much any blocking
                Duration::from_nanos(1)
            });
        let timeout_read = cmp::min(old_timeouts.0.unwrap_or(Duration::MAX), timeout);
        let timeout_write = cmp::min(old_timeouts.1.unwrap_or(Duration::MAX), timeout);
        if timeout_read == Duration::MAX && timeout_write == Duration::MAX {
            let res = work(self);
            return self.wrap_io_error(res, DeadlineErrorDuring::IO);
        }
        self.wrap_io_error(
            set_socket_timeouts(socket, (Some(timeout_read), Some(timeout_write))),
            DeadlineErrorDuring::SetTimeout,
        )?;
        let res = work(self);
        let res = self.wrap_io_error(res, DeadlineErrorDuring::IO);
        if let Err(err) = set_socket_timeouts(socket, old_timeouts) {
            warn!("failed to restore socket timeouts: {err}")
        }
        res
    }

    pub(crate) fn rewrite_error<T>(&self, res: Result<T>) -> Result<T> {
        if res.is_ok() {
            return res;
        }
        match self.error_during {
            Some(DeadlineErrorDuring::GetTimeout) => wrap_get_timeout_error(res),
            Some(DeadlineErrorDuring::SetTimeout) => wrap_set_timeout_error(res),
            Some(DeadlineErrorDuring::IO) | None => res,
        }
    }
}

fn get_socket_timeout(socket: &TcpStream) -> io::Result<(Option<Duration>, Option<Duration>)> {
    Ok((socket.read_timeout()?, socket.write_timeout()?))
}

fn set_socket_timeouts(
    socket: &TcpStream,
    timeouts: (Option<Duration>, Option<Duration>),
) -> io::Result<()> {
    let res1 = socket.set_read_timeout(timeouts.0);
    let res2 = socket.set_write_timeout(timeouts.1);
    match res1 {
        Ok(()) => res2,
        Err(e) => match res2 {
            Ok(()) => Err(e),
            Err(e2) => Err(io::Error::new(
                e.kind(),
                format!("multiple errors: {e}, {e2}"),
            )),
        },
    }
}

impl<S> Debug for DeadlineIO<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadlineIO")
            .field("deadline", &self.deadline)
            .field("socket", &self.socket)
            .finish()
    }
}

impl<S: Read + Write> Read for DeadlineIO<'_, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.with_deadline(|self_| self_.stream.read(buf))
    }
}

impl<S: Read + Write> Write for DeadlineIO<'_, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.with_deadline(|self_| self_.stream.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.with_deadline(|self_| self_.stream.flush())
    }
}
