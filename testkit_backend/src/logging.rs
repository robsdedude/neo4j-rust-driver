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

use std::io::{self, Cursor, Result as IoResult, Write};
use std::mem::swap;
use std::sync::{Arc, Mutex, OnceLock};

use log::LevelFilter;

#[derive(Debug, Default, Clone)]
struct LogBuffer(Arc<Mutex<Cursor<Vec<u8>>>>);

impl Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl LogBuffer {
    fn clear(&self) {
        self.0.lock().unwrap().get_mut().clear();
    }

    fn take_buffer(&self) -> Vec<u8> {
        let mut buffer = Default::default();
        swap(&mut buffer, &mut *self.0.lock().unwrap());
        buffer.into_inner()
    }
}

static LOG_BUFFER: OnceLock<LogBuffer> = OnceLock::new();

fn get_log_buffer() -> &'static LogBuffer {
    LOG_BUFFER.get_or_init(Default::default)
}

pub(super) fn init() {
    let buffer = get_log_buffer();
    fern::Dispatch::new()
        .chain(
            fern::Dispatch::new()
                .format(move |out, message, record| {
                    out.finish(format_args!(
                        "[{}][{:<7}] {}",
                        record.target(),
                        record.level(),
                        message
                    ))
                })
                .level(LevelFilter::Debug)
                .chain(io::stdout()),
        )
        .chain(
            fern::Dispatch::new()
                .format(move |out, message, record| {
                    out.finish(format_args!("[{:<7}] {}", record.level(), message))
                })
                .filter(|meta| !meta.target().starts_with("testkit_backend::"))
                .level(LevelFilter::Debug)
                .chain(Box::new(buffer.clone()) as Box<dyn Write + Send>),
        )
        .apply()
        .unwrap();
}

pub(super) fn clear_log() {
    get_log_buffer().clear();
}

pub(super) fn take_log() -> Vec<u8> {
    get_log_buffer().take_buffer()
}
