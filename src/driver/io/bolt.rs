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

mod bolt5x0;
mod chunk;
mod packstream;
mod response;

use log::Level;
use log::{debug, log_enabled};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::ops::Deref;
use std::result;

use usize_cast::FromUsize;

use crate::{Address, Neo4jError, Result, Value};
use bolt5x0::Bolt5x0StructTranslator;
use chunk::{Chunker, Dechunker};
pub use packstream::{
    PackStreamDeserialize, PackStreamDeserializer, PackStreamDeserializerImpl, PackStreamSerialize,
    PackStreamSerializer, PackStreamSerializerImpl,
};
use response::BoltResponse;
pub(crate) use response::{ResponseCallbacks, ResponseMessage};

// trait Bolt: Debug {
//     fn version(&self) -> (u8, u8) {
//         self.version
//     }
//
//     fn hello(
//         &mut self,
//         user_agent: &str,
//         auth: &HashMap<String, Value>,
//         routing_context: Option<&HashMap<String, Value>>,
//     ) -> Result<()>;
//     fn goodbye(&mut self) -> Result<()>;
//     fn reset(&mut self) -> Result<()>;
//     fn run(
//         &mut self,
//         query: &str,
//         parameters: Option<&HashMap<String, Value>>,
//         bookmarks: Option<&[String]>,
//         tx_timeout: Option<i64>,
//         tx_meta: Option<&HashMap<String, Value>>,
//         mode: Option<&str>,
//         db: Option<&str>,
//         imp_user: Option<&str>,
//     ) -> Result<()>;
//
//     fn read_one(&mut self) -> Result<()>;
//     fn write_one(&mut self) -> Result<()>;
//     fn read_all(&mut self) -> Result<()>;
//     fn write_all(&mut self) -> Result<()>;
// }

pub struct Bolt<R: Read, W: Write> {
    message_buff: VecDeque<Vec<u8>>,
    responses: VecDeque<BoltResponse>,
    reader: R,
    writer: W,
    socket: Option<TcpStream>,
    version: (u8, u8),
}

pub(crate) type TcpBolt = Bolt<BufReader<TcpStream>, BufWriter<TcpStream>>;

macro_rules! debug_buf_start {
    ($name:ident) => {
        let mut $name: Option<String> = match log_enabled!(Level::Debug) {
            true => Some(String::new()),
            false => None,
        };
    };
}

macro_rules! debug_buf {
    ($name:ident, $($args:tt)+) => {
        if log_enabled!(Level::Debug) {
            $name.as_mut().unwrap().push_str(&format!($($args)*))
        };
    }
}

macro_rules! debug_buf_end {
    ($name:ident) => {
        debug!("{}", $name.as_ref().map(|s| s.as_str()).unwrap_or(""));
    };
}

impl<R: Read, W: Write> Bolt<R, W> {
    fn new(version: (u8, u8), reader: R, writer: W, socket: Option<TcpStream>) -> Self {
        Self {
            message_buff: VecDeque::with_capacity(2048),
            responses: VecDeque::with_capacity(10),
            reader,
            writer,
            socket,
            version,
        }
    }

    // fn version(&self) -> (u8, u8) {
    //     self.version
    // }

    pub(crate) fn hello(
        &mut self,
        user_agent: &str,
        auth: &HashMap<String, Value>,
        routing_context: Option<&HashMap<String, Value>>,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO {{");
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        let translator = Bolt5x0StructTranslator {};
        serializer.write_struct_header(0x01, 1)?;
        let extra_size = 1 + routing_context.map(HashMap::len).unwrap_or(0) + auth.len();
        serializer.write_dict_header(u64::from_usize(extra_size))?;

        debug_buf!(log_buf, "{:?}: {:?}", "user_agent", user_agent);
        serializer.write_string("user_agent")?;
        serializer.write_string(user_agent)?;

        if let Some(routing_context) = routing_context {
            for (k, v) in routing_context {
                debug_buf!(log_buf, ", {:?}: {:?}", k, v);
                serializer.write_string(k)?;
                v.serialize(&mut serializer, &translator)?;
            }
        }

        for (k, v) in auth {
            if k == "credentials" {
                debug_buf!(log_buf, ", {:?}: {:?}", k, "**********");
            } else {
                debug_buf!(log_buf, ", {:?}: {:?}", k, v);
            }
            serializer.write_string(k)?;
            v.serialize(&mut serializer, &translator)?;
        }

        debug_buf!(log_buf, "}}");
        self.message_buff.push_back(message_buff);
        debug_buf_end!(log_buf);

        self.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Hello));
        Ok(())
    }

    pub(crate) fn goodbye(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x02, 0)?;
        self.message_buff.push_back(message_buff);
        debug!("C: GOODBYE");
        Ok(())
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x0F, 0)?;
        self.message_buff.push_back(message_buff);
        debug!("C: RESET");
        Ok(())
    }

    pub(crate) fn run<
        K1: Deref<Target = str> + Debug,
        S1: PackStreamSerialize,
        K2: Deref<Target = str> + Debug,
        S2: PackStreamSerialize,
    >(
        &mut self,
        query: &str,
        parameters: Option<&HashMap<K1, S1>>,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_meta: Option<&HashMap<K2, S2>>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: RUN");
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        let translator = Bolt5x0StructTranslator {};
        serializer.write_struct_header(0x10, 3)?;

        debug_buf!(log_buf, " {:?}", query);
        serializer.write_string(query)?;

        debug_buf!(log_buf, " {:?}", &parameters);
        match parameters {
            Some(parameters) => serializer.write_dict(&translator, parameters)?,
            None => serializer.write_dict_header(0)?,
        }

        debug_buf!(log_buf, " {{");
        let mut sep = "";
        serializer.write_dict_header(
            (bookmarks.map_or(false, |b| !b.is_empty()) as u64)
                + (tx_timeout.is_some() as u64)
                + (tx_meta.map_or(false, |m| !m.is_empty()) as u64)
                + (mode.is_some() as u64)
                + (db.is_some() as u64)
                + (imp_user.is_some() as u64),
        )?;
        if let Some(bookmarks) = bookmarks {
            if !bookmarks.is_empty() {
                debug_buf!(log_buf, "{:?}: {:?}", "bookmarks", bookmarks);
                sep = ", ";
                serializer.write_string("bookmarks")?;
                bookmarks.serialize(&mut serializer, &translator)?;
            }
        }
        if let Some(tx_timeout) = tx_timeout {
            debug_buf!(log_buf, "{}{:?}: {:?}", sep, "tx_timeout", tx_timeout);
            sep = ", ";
            serializer.write_string("tx_timeout")?;
            tx_timeout.serialize(&mut serializer, &translator)?;
        }
        if let Some(tx_meta) = tx_meta {
            if !tx_meta.is_empty() {
                debug_buf!(log_buf, "{}{:?}: {:?}", sep, "tx_meta", tx_meta);
                sep = ", ";
                serializer.write_string("tx_meta")?;
                tx_meta.serialize(&mut serializer, &translator)?;
            }
        }
        if let Some(mode) = mode {
            debug_buf!(log_buf, "{}{:?}: {:?}", sep, "mode", mode);
            sep = ", ";
            serializer.write_string("mode")?;
            mode.serialize(&mut serializer, &translator)?;
        }
        if let Some(db) = db {
            debug_buf!(log_buf, "{}{:?}: {:?}", sep, "db", db);
            sep = ", ";
            serializer.write_string("db")?;
            db.serialize(&mut serializer, &translator)?;
        }
        if let Some(imp_user) = imp_user {
            debug_buf!(log_buf, "{}{:?}: {:?}", sep, "imp_user", imp_user);
            sep = ", ";
            serializer.write_string("imp_user")?;
            imp_user.serialize(&mut serializer, &translator)?;
        }
        self.message_buff.push_back(message_buff);
        debug_buf!(log_buf, "}}");

        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Run, callbacks));
        debug_buf_end!(log_buf);
        Ok(())
    }

    pub(crate) fn discard(&mut self, n: i64, qid: i64) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x2F, 1)?;

        serializer.write_dict_header(2)?;
        serializer.write_string("n")?;
        serializer.write_int(n)?;
        serializer.write_string("qid")?;
        serializer.write_int(qid)?;

        self.message_buff.push_back(message_buff);
        debug!("C: DISCARD {{{:?}: {:?}, {:?}: {:?}}}", "n", n, "qid", qid);
        Ok(())
    }

    pub(crate) fn pull(&mut self, n: i64, qid: i64) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x3F, 1)?;

        serializer.write_dict_header(2)?;
        serializer.write_string("n")?;
        serializer.write_int(n)?;
        serializer.write_string("qid")?;
        serializer.write_int(qid)?;

        self.message_buff.push_back(message_buff);
        debug!("C: PULL {{{:?}: {:?}, {:?}: {:?}}}", "n", n, "qid", qid);
        Ok(())
    }

    pub(crate) fn read_one(&mut self) -> Result<()> {
        let response = self
            .responses
            .pop_front()
            .expect("called Bolt::read_one with empty response queue");
        let mut dechunker = Dechunker::new(&mut self.reader);
        let mut deserializer = PackStreamDeserializerImpl::new(&mut dechunker);
        let translator = Bolt5x0StructTranslator {};
        let message = deserializer.load::<Value, _>(&translator)?;
        let _: i64 = "a"; // forced compilation error to find where I want to pick up again

        todo!();
        // TODO: decode
        //       - use custom message decoder (per Bolt version) wrapping the Dechunker
        // TODO: logging
        Ok(())
    }

    pub(crate) fn write_one(&mut self) -> Result<()> {
        if let Some(message_buff) = self.message_buff.pop_front() {
            let mut chunker = Chunker::new(&message_buff);
            for chunk in chunker {
                self.writer.write_all(&chunk)?
            }
        }
        Ok(())
    }

    pub(crate) fn read_all(&mut self) -> Result<()> {
        while self.expects_reply() {
            self.read_one()?
        }
        Ok(())
    }

    pub(crate) fn write_all(&mut self) -> Result<()> {
        while self.has_buffered_message() {
            self.write_one()?
        }
        Ok(())
    }

    fn has_buffered_message(&self) -> bool {
        !self.message_buff.is_empty()
    }

    fn expects_reply(&self) -> bool {
        !self.responses.is_empty()
    }
}

impl<R: Read, W: Write> Debug for Bolt<R, W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Bolt5x0 {{\n  message_buff: {:?}\n  responses: {:?}\n}}",
            self.message_buff, self.responses
        )
    }
}

pub trait BoltStructTranslator {
    fn serialize_point_2d<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        x: f64,
        y: f64,
    ) -> result::Result<(), S::Error>;
    fn serialize_point_3d<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        x: f64,
        y: f64,
        z: f64,
    ) -> result::Result<(), S::Error>;

    fn deserialize_struct<V: PackStreamDeserialize>(
        &self,
        tag: u8,
        fields: Vec<V::Value>,
    ) -> V::Value;
}

const BOLT_MAGIC_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const BOLT_VERSION_OFFER: [u8; 16] = [
    0, 0, 0, 5, // BOLT 5.0
    0, 0, 0, 0, // -
    0, 0, 0, 0, // -
    0, 0, 0, 0, // -
];

pub(crate) fn open(address: &Address) -> Result<TcpBolt> {
    let mut stream = TcpStream::connect(address)?;

    // TODO: TLS

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream.try_clone()?);

    debug!("C: <HANDSHAKE> {:02X?}", BOLT_MAGIC_PREAMBLE);
    writer.write_all(&BOLT_MAGIC_PREAMBLE)?;
    debug!("C: <BOLT> {:02X?}", BOLT_VERSION_OFFER);
    writer.write_all(&BOLT_VERSION_OFFER)?;
    writer.flush()?;

    let mut negotiated_version = [0u8; 4];
    reader.read_exact(&mut negotiated_version)?;
    debug!("S: <BOLT> {:02X?}", negotiated_version);

    let version = match negotiated_version {
        [0, 0, 0, 0] => Err(Neo4jError::InvalidConfig {
            message: String::from("Server version not supported."),
        }),
        [0, 0, 0, 5] => Ok((5, 0)),
        [72, 84, 84, 80] => {
            // "HTTP"
            Err(Neo4jError::InvalidConfig {
                message: format!(
                    "Unexpected server handshake response {:?} (looks like HTTP)",
                    &negotiated_version
                ),
            })
        }
        _ => Err(Neo4jError::InvalidConfig {
            message: format!(
                "Unexpected server handshake response {:?}",
                &negotiated_version
            ),
        }),
    }?;

    Ok(Bolt::new(version, reader, writer, Some(stream)))
}
