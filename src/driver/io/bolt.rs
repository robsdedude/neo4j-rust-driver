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
mod bolt_state;
mod chunk;
mod message;
mod packstream;
mod response;

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::ops::DerefMut;
use std::{io, result};

use log::{debug, log_enabled, Level};
use usize_cast::FromUsize;

use crate::{Address, Neo4jError, Result, Value};
use bolt5x0::Bolt5x0StructTranslator;
use bolt_state::{BoltState, BoltStateTracker};
use chunk::{Chunker, Dechunker};
use message::BoltMessage;
pub use packstream::{PackStreamDeserialize, PackStreamSerialize};
pub(crate) use packstream::{
    PackStreamDeserializer, PackStreamDeserializerImpl, PackStreamSerializer,
    PackStreamSerializerImpl,
};
pub(crate) use response::{
    BoltMeta, BoltRecordFields, BoltResponse, ResponseCallbacks, ResponseMessage,
};

pub struct Bolt<R: Read, W: Write> {
    message_buff: VecDeque<Vec<Vec<u8>>>,
    responses: VecDeque<BoltResponse>,
    reader: R,
    writer: W,
    socket: Option<TcpStream>,
    version: (u8, u8),
    closed: bool,
    state: BoltStateTracker,
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
            closed: false,
            state: BoltStateTracker::new(version),
        }
    }

    // fn version(&self) -> (u8, u8) {
    //     self.version
    // }

    pub(crate) fn closed(&self) -> bool {
        self.closed
    }

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
        let extra_size = 1 + <bool as Into<usize>>::into(routing_context.is_some()) + auth.len();
        serializer.write_dict_header(u64::from_usize(extra_size))?;

        debug_buf!(log_buf, "{:?}: {:?}", "user_agent", user_agent);
        serializer.write_string("user_agent")?;
        serializer.write_string(user_agent)?;

        if let Some(routing_context) = routing_context {
            debug_buf!(log_buf, ", \"routing\": {:?}", routing_context);
            routing_context.serialize(&mut serializer, &translator)?;
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
        self.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(log_buf);

        self.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Hello));
        Ok(())
    }

    pub(crate) fn goodbye(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x02, 0)?;
        self.message_buff.push_back(vec![message_buff]);
        self.closed = true;
        debug!("C: GOODBYE");
        Ok(())
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x0F, 0)?;
        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Reset));
        debug!("C: RESET");
        Ok(())
    }

    pub(crate) fn run_prepare(
        &self,
        query: &str,
        bookmarks: Option<&[String]>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<RunPreparation> {
        RunPreparation::new(query, bookmarks, mode, db, imp_user, self.version)
    }

    pub(crate) fn run_submit(&mut self, run_prep: RunPreparation, callbacks: ResponseCallbacks) {
        assert_eq!(run_prep.bolt_version, self.version);
        self.message_buff.push_back(run_prep.into_message_buff());
        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Run, callbacks));
    }

    pub(crate) fn discard(&mut self, n: i64, qid: i64, callbacks: ResponseCallbacks) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x2F, 1)?;

        serializer.write_dict_header(2)?;
        serializer.write_string("n")?;
        serializer.write_int(n)?;
        serializer.write_string("qid")?;
        serializer.write_int(qid)?;

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Discard, callbacks));
        debug!("C: DISCARD {{{:?}: {:?}, {:?}: {:?}}}", "n", n, "qid", qid);
        Ok(())
    }

    pub(crate) fn pull(&mut self, n: i64, qid: i64, callbacks: ResponseCallbacks) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x3F, 1)?;

        serializer.write_dict_header(2)?;
        serializer.write_string("n")?;
        serializer.write_int(n)?;
        serializer.write_string("qid")?;
        serializer.write_int(qid)?;

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Pull, callbacks));
        debug!("C: PULL {{{:?}: {:?}, {:?}: {:?}}}", "n", n, "qid", qid);
        Ok(())
    }

    pub(crate) fn route(
        &mut self,
        routing_context: &HashMap<String, Value>,
        bookmarks: Option<&[String]>,
        db: Option<&str>,
        imp_user: Option<&str>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: ROUTE ");
        let translator = Bolt5x0StructTranslator {};
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x66, 3)?;

        debug_buf!(log_buf, "{:?} ", routing_context);
        serializer.write_dict(&translator, routing_context)?;
        match bookmarks {
            None => {
                debug_buf!(log_buf, "[] ");
                serializer.write_list_header(0)?;
            }
            Some(bms) => {
                debug_buf!(log_buf, "{:?} ", bms);
                serializer.write_list(&translator, bms)?;
            }
        }

        debug_buf!(log_buf, "{{");
        let extra_size = <bool as Into<usize>>::into(db.is_some())
            + <bool as Into<usize>>::into(imp_user.is_some());
        serializer.write_dict_header(u64::from_usize(extra_size))?;

        let mut sep = "";
        if let Some(db) = db {
            sep = ", ";
            debug_buf!(log_buf, "\"db\": {:?}", db);
            serializer.write_string(db)?;
        }

        if let Some(imp_user) = imp_user {
            debug_buf!(log_buf, "{}\"imp_user\": {:?}", sep, imp_user);
            serializer.write_string(imp_user)?;
        }
        debug_buf!(log_buf, "}}");

        debug_buf_end!(log_buf);
        Ok(())
    }

    pub(crate) fn read_one(&mut self) -> Result<()> {
        let mut response = self
            .responses
            .pop_front()
            .expect("called Bolt::read_one with empty response queue");

        let mut dechunker = Dechunker::new(&mut self.reader, || {
            self.closed = true;
            self.socket.as_ref().map(|s| s.shutdown(Shutdown::Both));
        });
        // let mut deserializer = PackStreamDeserializerImpl::new(&mut dechunker);
        let translator = Bolt5x0StructTranslator {};
        let message: BoltMessage<Value> = BoltMessage::load(&mut dechunker, |r| {
            let mut deserializer = PackStreamDeserializerImpl::new(r);
            Ok(deserializer.load::<Value, _>(&translator)?)
        })?;
        match message {
            BoltMessage {
                tag: 0x70,
                mut fields,
            } => {
                // SUCCESS
                Self::assert_response_field_count("SUCCESS", &fields, 1)?;
                let meta = fields.pop().unwrap();
                debug!("S: SUCCESS {:?}", meta);
                self.state.success(response.message, &meta);
                response.callbacks.on_success(meta)
            }
            BoltMessage { tag: 0x7E, fields } => {
                // IGNORED
                Self::assert_response_field_count("IGNORED", &fields, 0)?;
                debug!("S: IGNORED");
                response.callbacks.on_ignored()
            }
            BoltMessage {
                tag: 0x7F,
                mut fields,
            } => {
                // FAILURE
                Self::assert_response_field_count("FAILURE", &fields, 1)?;
                let meta = fields.pop().unwrap();
                debug!("S: FAILURE {:?}", meta);
                self.state.failure();
                response.callbacks.on_failure(meta)
            }
            BoltMessage {
                tag: 0x71,
                mut fields,
            } => {
                // RECORD
                Self::assert_response_field_count("RECORD", &fields, 1)?;
                let data = fields.pop().unwrap();
                debug!("S: RECORD [...]");
                let res = response.callbacks.on_record(data);
                self.responses.push_front(response);
                res
            }
            BoltMessage { tag, .. } => Err(Neo4jError::ProtocolError {
                message: format!("unknown response message tag {:02X?}", tag),
            }),
        }
    }

    fn assert_response_field_count<T>(
        name: &str,
        fields: &[T],
        expected_count: usize,
    ) -> Result<()> {
        if fields.len() == expected_count {
            Ok(())
        } else {
            Err(Neo4jError::ProtocolError {
                message: format!(
                    "{} response should have {} field but found {:?}",
                    name,
                    expected_count,
                    fields.len()
                ),
            })
        }
    }

    pub(crate) fn write_one(&mut self) -> Result<()> {
        if let Some(message_buff) = self.message_buff.pop_front() {
            let chunker = Chunker::new(&message_buff);
            for chunk in chunker {
                if let Err(e) = self.writer.write_all(&chunk) {
                    self.closed = true;
                    self.socket.as_ref().map(|s| s.shutdown(Shutdown::Both));
                    return Err(Neo4jError::write_error(e));
                }
            }
        }
        Neo4jError::wrap_write(self.writer.flush())?;
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

    pub(crate) fn has_buffered_message(&self) -> bool {
        !self.message_buff.is_empty()
    }

    pub(crate) fn expects_reply(&self) -> bool {
        !self.responses.is_empty()
    }

    pub(crate) fn needs_reset(&self) -> bool {
        if let Some(response) = self.responses.iter().last() {
            if response.message == ResponseMessage::Reset {
                return false;
            }
        }
        !(self.state.state() == BoltState::Ready && self.responses.is_empty())
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

impl<R: Read, W: Write> Drop for Bolt<R, W> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.message_buff.clear();
        self.responses.clear();
        if let Err(_) = self.goodbye() {
            return;
        }
        let _ = self.write_all();
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
    let stream = Neo4jError::wrap_connect(TcpStream::connect(address))?;

    // TODO: TLS

    let mut reader = BufReader::new(Neo4jError::wrap_connect(stream.try_clone())?);
    let mut writer = BufWriter::new(Neo4jError::wrap_connect(stream.try_clone())?);

    debug!("C: <HANDSHAKE> {:02X?}", BOLT_MAGIC_PREAMBLE);
    Neo4jError::wrap_write(writer.write_all(&BOLT_MAGIC_PREAMBLE))?;
    debug!("C: <BOLT> {:02X?}", BOLT_VERSION_OFFER);
    Neo4jError::wrap_write(writer.write_all(&BOLT_VERSION_OFFER))?;
    Neo4jError::wrap_write(writer.flush())?;

    let mut negotiated_version = [0u8; 4];
    Neo4jError::wrap_read(reader.read_exact(&mut negotiated_version))?;
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

const RUN_PREP_MSG_IDX: usize = 0;
const RUN_PREP_PARAMS_IDX: usize = 1;
const RUN_PREP_EXTRA_HEADER_IDX: usize = 2;
const RUN_PREP_EXTRA_ELEMS_IDX: usize = 3;
const RUN_PREP_EXTRA_TIMEOUT_IDX: usize = 4;
const RUN_PREP_EXTRA_META_IDX: usize = 5;
const RUN_PREP_SIZE: usize = 6;

#[derive(Debug)]
pub(crate) struct RunPreparation {
    buffers: Vec<Vec<u8>>,
    extra_size: u64,
    log_msg: Option<[String; RUN_PREP_SIZE]>,
    bolt_version: (u8, u8),
}

impl RunPreparation {
    fn new(
        query: &str,
        bookmarks: Option<&[String]>,
        // tx_timeout: Option<i64>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
        bolt_version: (u8, u8),
    ) -> Result<Self> {
        let translator = Bolt5x0StructTranslator {};
        let mut buffers = Vec::with_capacity(RUN_PREP_SIZE);

        let mut log_msg = match log_enabled!(Level::Debug) {
            true => Some([(); RUN_PREP_SIZE].map(|_| String::new())),
            false => None,
        };

        let mut log_head = log_msg.as_mut().map(|m| &mut m[RUN_PREP_MSG_IDX]);
        debug_buf!(log_head, "C: RUN {} ", query);
        let mut buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut buff);
        serializer.write_struct_header(0x10, 3)?;
        serializer.write_string(query)?;
        buffers.push(buff);

        let mut log_params = log_msg.as_mut().map(|m| &mut m[RUN_PREP_PARAMS_IDX]);
        debug_buf!(log_params, "{{}} ");
        let mut buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut buff);
        serializer.write_dict_header(0)?;
        buffers.push(buff);

        let mut log_extra_header = log_msg.as_mut().map(|m| &mut m[RUN_PREP_EXTRA_HEADER_IDX]);
        debug_buf!(log_extra_header, "{{");
        let mut buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut buff);
        let extra_size = (bookmarks.map_or(false, |b| !b.is_empty()) as u64)
            + (mode.is_some() as u64)
            + (db.is_some() as u64)
            + (imp_user.is_some() as u64);
        serializer.write_dict_header(extra_size)?;
        buffers.push(buff);

        let mut log_sep = "";
        let mut log_extra_elems = log_msg.as_mut().map(|m| &mut m[RUN_PREP_EXTRA_ELEMS_IDX]);
        let mut buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut buff);
        if let Some(bookmarks) = bookmarks {
            if !bookmarks.is_empty() {
                debug_buf!(log_extra_elems, "{:?}: {:?}", "bookmarks", bookmarks);
                log_sep = ", ";
                serializer.write_string("bookmarks")?;
                bookmarks.serialize(&mut serializer, &translator)?;
            }
        }
        if let Some(mode) = mode {
            debug_buf!(log_extra_elems, "{}{:?}: {:?}", log_sep, "mode", mode);
            log_sep = ", ";
            serializer.write_string("mode")?;
            mode.serialize(&mut serializer, &translator)?;
        }
        if let Some(db) = db {
            debug_buf!(log_extra_elems, "{}{:?}: {:?}", log_sep, "db", db);
            log_sep = ", ";
            serializer.write_string("db")?;
            db.serialize(&mut serializer, &translator)?;
        }
        if let Some(imp_user) = imp_user {
            debug_buf!(
                log_extra_elems,
                "{}{:?}: {:?}",
                log_sep,
                "imp_user",
                imp_user
            );
            serializer.write_string("imp_user")?;
            imp_user.serialize(&mut serializer, &translator)?;
        }

        // timeout
        buffers.push(Vec::new());

        // meta
        buffers.push(Vec::new());

        Ok(Self {
            buffers,
            extra_size,
            log_msg,
            bolt_version,
        })
    }

    pub(crate) fn with_parameters<K: AsRef<str> + Debug, S: PackStreamSerialize>(
        &mut self,
        parameters: &HashMap<K, S>,
    ) -> Result<()> {
        let translator = Bolt5x0StructTranslator {};

        let mut log = self.log_msg.as_mut().map(|m| &mut m[RUN_PREP_PARAMS_IDX]);
        log.as_mut().map(|s| s.clear());
        debug_buf!(log, "{:?} ", parameters);

        let buff = &mut self.buffers[RUN_PREP_PARAMS_IDX];
        buff.clear();
        let mut serializer = PackStreamSerializerImpl::new(buff);
        serializer.write_dict(&translator, parameters)?;

        Ok(())
    }

    pub(crate) fn with_tx_meta<K: AsRef<str> + Debug, S: PackStreamSerialize>(
        &mut self,
        tx_meta: &HashMap<K, S>,
    ) -> Result<()> {
        let translator = Bolt5x0StructTranslator {};

        let first_time = self.buffers[RUN_PREP_EXTRA_META_IDX].is_empty();
        if first_time {
            self.extra_size += 1;
            let buff = &mut self.buffers[RUN_PREP_EXTRA_HEADER_IDX];
            let mut serializer = PackStreamSerializerImpl::new(buff);
            serializer.write_dict_header(self.extra_size)?;
        }

        let mut log = self
            .log_msg
            .as_mut()
            .map(|m| &mut m[RUN_PREP_EXTRA_META_IDX]);
        log.as_mut().map(|s| s.clear());
        debug_buf!(log, "{:?}: {:?}", "tx_meta", tx_meta);

        let buff = &mut self.buffers[RUN_PREP_EXTRA_META_IDX];
        buff.clear();
        let mut serializer = PackStreamSerializerImpl::new(buff);
        serializer.write_string("tx_meta")?;
        tx_meta.serialize(&mut serializer, &translator)?;

        Ok(())
    }

    pub(crate) fn with_tx_timeout(&mut self, tx_timeout: i64) -> Result<()> {
        let translator = Bolt5x0StructTranslator {};

        let first_time = self.buffers[RUN_PREP_EXTRA_TIMEOUT_IDX].is_empty();
        if first_time {
            self.extra_size += 1;
            let buff = &mut self.buffers[RUN_PREP_EXTRA_HEADER_IDX];
            let mut serializer = PackStreamSerializerImpl::new(buff);
            serializer.write_dict_header(self.extra_size)?;
        }

        let mut log = self
            .log_msg
            .as_mut()
            .map(|m| &mut m[RUN_PREP_EXTRA_TIMEOUT_IDX]);
        log.as_mut().map(|s| s.clear());
        debug_buf!(log, "{:?}: {:?}", "tx_timeout", tx_timeout);

        let buff = &mut self.buffers[RUN_PREP_EXTRA_TIMEOUT_IDX];
        let mut serializer = PackStreamSerializerImpl::new(buff);
        serializer.write_string("tx_timeout")?;
        tx_timeout.serialize(&mut serializer, &translator)?;

        Ok(())
    }

    fn into_message_buff(self) -> Vec<Vec<u8>> {
        if log_enabled!(Level::Debug) {
            let log_msg = self.borrow().log_msg.as_ref().unwrap();
            let mut log_buff =
                String::with_capacity(log_msg.iter().map(|m| m.len()).sum::<usize>() + 5);
            for msg in log_msg[..=RUN_PREP_EXTRA_ELEMS_IDX].iter()
            // .iter()
            // .take(RUN_PREP_EXTRA_ELEMS_IDX + 1)
            {
                log_buff.push_str(msg);
            }
            let mut sep = if log_msg[RUN_PREP_EXTRA_ELEMS_IDX].is_empty() {
                ""
            } else {
                ", "
            };
            for msg in log_msg[RUN_PREP_EXTRA_TIMEOUT_IDX..]
                .iter()
                .filter(|m| !m.is_empty())
            {
                log_buff.push_str(sep);
                log_buff.push_str(msg);
                sep = ", ";
            }
            log_buff.push('}');
            debug!("{}", log_buff);
        }
        self.buffers
    }
}
