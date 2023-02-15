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
use std::io::Read;
use std::ops::Deref;

use usize_cast::IntoUsize;

pub(crate) struct Chunker<'a> {
    buf: &'a [u8],
    chunk_size: [u8; 2],
    in_chunk: bool,
    ended: bool,
}

impl<'a> Chunker<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Chunker {
            buf,
            chunk_size: [0; 2],
            in_chunk: false,
            ended: false,
        }
    }
}

impl<'a> Iterator for Chunker<'a> {
    type Item = Chunk<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.ended {
            if !self.buf.is_empty() {
                let end = cmp::min(self.buf.len(), u16::MAX.into_usize());
                if self.in_chunk {
                    let (chunk, new_buf) = self.buf.split_at(end);
                    self.buf = new_buf;
                    Some(Chunk::Buffer(chunk))
                } else {
                    let size = (end as u16).to_be_bytes();
                    Some(Chunk::Size(size))
                }
            } else {
                self.ended = true;
                Some(Chunk::Size([0, 0])) // terminate message with empty chunk
            }
        } else {
            None
        }
    }
}

pub(crate) enum Chunk<'a> {
    Buffer(&'a [u8]),
    Size([u8; 2]),
}

impl<'a> Deref for Chunk<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Chunk::Buffer(buf) => buf,
            Chunk::Size(size) => size,
        }
    }
}

pub(crate) struct Dechunker<R: Read, F: FnMut()> {
    reader: R,
    current_chunk_size: Option<usize>,
    on_error: F,
}

impl<R: Read, F: FnMut()> Dechunker<R, F> {
    pub(crate) fn new(reader: R, on_error: F) -> Self {
        Self {
            reader,
            current_chunk_size: None,
            on_error,
        }
    }

    fn error_wrap<T, E>(&mut self, res: Result<T, E>) -> Result<T, E> {
        if res.is_err() {
            (self.on_error)();
        }
        res
    }
}

impl<R: Read, F: FnMut()> Read for Dechunker<R, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.current_chunk_size.is_none() {
            let mut size_buf = [0; 2];
            let res = self.reader.read_exact(&mut size_buf);
            self.error_wrap(res)?;
            self.current_chunk_size = Some(u16::from_be_bytes(size_buf).into_usize());
        }
        let new_buf_size = cmp::min(buf.len(), self.current_chunk_size.unwrap());
        let buf = &mut buf[..new_buf_size];
        let res = self.reader.read(buf);
        self.error_wrap(res)
    }
}