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
pub(crate) mod message_parameters;
mod packstream;
mod response;

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::mem;
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::ops::Deref;
use std::result;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atomic_refcell::AtomicRefCell;
use log::{debug, log_enabled, warn, Level};
use usize_cast::FromUsize;

use crate::driver::io::deadline::DeadlineIO;
use crate::{Address, Neo4jError, Result, ValueReceive, ValueSend};
use bolt5x0::Bolt5x0StructTranslator;
use bolt_state::{BoltState, BoltStateTracker};
use chunk::{Chunker, Dechunker};
use message::BoltMessage;
use message_parameters::RunParameters;
pub(crate) use packstream::{
    PackStreamDeserializer, PackStreamDeserializerImpl, PackStreamSerializer,
    PackStreamSerializerDebugImpl, PackStreamSerializerImpl,
};
pub(crate) use response::{
    BoltMeta, BoltRecordFields, BoltResponse, ResponseCallbacks, ResponseMessage,
};

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

macro_rules! bolt_debug_extra {
    ($bolt:expr) => {
        'a: {
            let meta = $bolt.meta.try_borrow();
            // ugly format because rust-fmt is broken
            let Ok(meta) = meta else { break 'a dbg_extra($bolt.local_port, Some("!!!!")); };
            let Some(ValueReceive::String(id)) = meta.get("connection_id") else { break 'a dbg_extra($bolt.local_port, None); };
            dbg_extra($bolt.local_port, Some(id))
        }
    };
}

macro_rules! debug_buf_end {
    ($bolt:expr, $name:ident) => {
        debug!(
            "{}{}",
            bolt_debug_extra!($bolt),
            $name.as_ref().map(|s| s.as_str()).unwrap_or("")
        );
    };
}

macro_rules! bolt_debug {
    ($bolt:expr, $($args:tt)+) => {
        debug!(
            "{}{}",
            bolt_debug_extra!($bolt),
            format!($($args)*)
        );
    };
}

macro_rules! socket_debug {
    ($local_port:expr, $($args:tt)+) => {
        debug!(
            "{}{}",
            dbg_extra(Some($local_port), None),
            format!($($args)*)
        );
    };
}

fn dbg_extra(port: Option<u16>, bolt_id: Option<&str>) -> String {
    format!(
        "[#{:04X} {:<10}] ",
        port.unwrap_or(0),
        bolt_id.unwrap_or("")
    )
}

const SERVER_AGENT_KEY: &str = "server";

pub enum ConnectionState {
    Healthy,
    Broken,
    Closed,
}

pub struct Bolt<R: Read, W: Write> {
    message_buff: VecDeque<Vec<Vec<u8>>>,
    responses: VecDeque<BoltResponse>,
    reader: R,
    writer: W,
    socket: Option<TcpStream>,
    local_port: Option<u16>,
    version: (u8, u8),
    connection_state: ConnectionState,
    bolt_state: BoltStateTracker,
    meta: Arc<AtomicRefCell<HashMap<String, ValueReceive>>>,
    server_agent: Arc<AtomicRefCell<Arc<String>>>,
    address: Arc<Address>,
    last_qid: Arc<AtomicRefCell<Option<i64>>>,
}

pub(crate) type TcpBolt = Bolt<BufReader<TcpStream>, BufWriter<TcpStream>>;

impl<R: Read, W: Write> Bolt<R, W> {
    fn new(
        version: (u8, u8),
        reader: R,
        writer: W,
        socket: Option<TcpStream>,
        local_port: Option<u16>,
        address: Arc<Address>,
    ) -> Self {
        Self {
            message_buff: VecDeque::with_capacity(2048),
            responses: VecDeque::with_capacity(10),
            reader,
            writer,
            socket,
            local_port,
            version,
            connection_state: ConnectionState::Healthy,
            bolt_state: BoltStateTracker::new(version),
            meta: Default::default(),
            server_agent: Default::default(),
            address,
            last_qid: Default::default(),
        }
    }

    fn dbg_extra(&self) -> String {
        let meta = self.meta.try_borrow();
        let Ok(meta) = meta else {
             return dbg_extra(self.local_port, Some("!!!!"));
        };
        let Some(ValueReceive::String(id)) = meta.get("connection_id") else {
            return dbg_extra(self.local_port, None);
        };
        dbg_extra(self.local_port, Some(id))
    }

    pub(crate) fn closed(&self) -> bool {
        !matches!(self.connection_state, ConnectionState::Healthy)
    }

    pub(crate) fn unexpectedly_closed(&self) -> bool {
        matches!(self.connection_state, ConnectionState::Broken)
            && matches!(self.bolt_state.state(), BoltState::Failed)
    }

    pub(crate) fn protocol_version(&self) -> (u8, u8) {
        self.version
    }

    pub(crate) fn address(&self) -> Arc<Address> {
        Arc::clone(&self.address)
    }

    pub(crate) fn server_agent(&self) -> Arc<String> {
        Arc::clone(self.server_agent.deref().borrow().deref())
    }

    pub(crate) fn hello(
        &mut self,
        user_agent: &str,
        auth: &HashMap<String, ValueSend>,
        routing_context: Option<&HashMap<String, ValueSend>>,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        let translator = Bolt5x0StructTranslator {};
        serializer.write_struct_header(0x01, 1)?;

        let extra_size =
            1 + <bool as Into<u64>>::into(routing_context.is_some()) + u64::from_usize(auth.len());
        serializer.write_dict_header(extra_size)?;
        serializer.write_string("user_agent")?;
        serializer.write_string(user_agent)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.write_string("user_agent").unwrap();
            dbg_serializer.write_string(user_agent).unwrap();
            dbg_serializer.flush()
        });

        if let Some(routing_context) = routing_context {
            serializer.write_string("routing")?;
            self.serialize_dict(&mut serializer, &translator, routing_context)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("routing").unwrap();
                self.serialize_dict(&mut dbg_serializer, &translator, routing_context)
                    .unwrap();
                dbg_serializer.flush()
            });
        }

        for (k, v) in auth {
            serializer.write_string(k)?;
            self.serialize_value(&mut serializer, &translator, v)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string(k).unwrap();
                if k == "credentials" {
                    dbg_serializer.write_string("**********").unwrap();
                } else {
                    self.serialize_value(&mut dbg_serializer, &translator, v)
                        .unwrap();
                }
                dbg_serializer.flush()
            });
        }

        self.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(self, log_buf);

        let self_meta = Arc::clone(&self.meta);
        let self_server_agent = Arc::clone(&self.server_agent);
        self.responses.push_back(BoltResponse::new(
            ResponseMessage::Hello,
            ResponseCallbacks::new().with_on_success(move |mut meta| {
                if let Some((key, value)) = meta.remove_entry(SERVER_AGENT_KEY) {
                    match value {
                        ValueReceive::String(value) => {
                            mem::swap(&mut *self_server_agent.borrow_mut(), &mut Arc::new(value));
                        }
                        _ => {
                            warn!("Server sent unexpected server_agent type {:?}", &value);
                            meta.insert(key, value);
                        }
                    }
                }
                mem::swap(&mut *self_meta.borrow_mut(), &mut meta);
                Ok(())
            }),
        ));
        Ok(())
    }

    pub(crate) fn goodbye(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x02, 0)?;
        self.message_buff.push_back(vec![message_buff]);
        self.connection_state = ConnectionState::Closed;
        bolt_debug!(self, "C: GOODBYE");
        Ok(())
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x0F, 0)?;
        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Reset));
        bolt_debug!(self, "C: RESET");
        Ok(())
    }

    pub(crate) fn run<KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        parameters: RunParameters<KP, KM>,
        mut callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let RunParameters {
            query,
            parameters,
            bookmarks,
            tx_timeout,
            tx_metadata,
            mode,
            db,
            imp_user,
        } = parameters;

        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: RUN");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        let translator = Bolt5x0StructTranslator {};
        serializer.write_struct_header(0x10, 3)?;

        serializer.write_string(query)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_string(query).unwrap();
            dbg_serializer.flush()
        });

        match parameters {
            Some(parameters) => {
                self.serialize_dict(&mut serializer, &translator, parameters)?;
                debug_buf!(log_buf, " {}", {
                    self.serialize_dict(&mut dbg_serializer, &translator, parameters)
                        .unwrap();
                    dbg_serializer.flush()
                });
            }
            None => {
                serializer.write_dict_header(0)?;
                debug_buf!(log_buf, " {}", {
                    dbg_serializer.write_dict_header(0).unwrap();
                    dbg_serializer.flush()
                });
            }
        }

        let extra_size = [
            bookmarks.is_some() && !bookmarks.unwrap().is_empty(),
            tx_timeout.is_some(),
            tx_metadata.is_some() && !tx_metadata.unwrap().is_empty(),
            mode.is_some() && mode.unwrap() != "w",
            db.is_some(),
            imp_user.is_some(),
        ]
        .into_iter()
        .map(<bool as Into<u64>>::into)
        .sum();

        serializer.write_dict_header(extra_size)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.flush()
        });

        if let Some(bookmarks) = bookmarks {
            if !bookmarks.is_empty() {
                serializer.write_string("bookmarks")?;
                self.serialize_str_slice(&mut serializer, bookmarks)?;
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("bookmarks").unwrap();
                    self.serialize_str_slice(&mut dbg_serializer, bookmarks)
                        .unwrap();
                    dbg_serializer.flush()
                });
            }
        }

        if let Some(tx_timeout) = tx_timeout {
            serializer.write_string("tx_timeout")?;
            serializer.write_int(tx_timeout)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("tx_timeout").unwrap();
                dbg_serializer.write_int(tx_timeout).unwrap();
                dbg_serializer.flush()
            });
        }

        if let Some(tx_metadata) = tx_metadata {
            if !tx_metadata.is_empty() {
                serializer.write_string("tx_metadata")?;
                self.serialize_dict(&mut serializer, &translator, tx_metadata)?;
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("tx_metadata").unwrap();
                    self.serialize_dict(&mut dbg_serializer, &translator, tx_metadata)
                        .unwrap();
                    dbg_serializer.flush()
                });
            }
        }

        if let Some(mode) = mode {
            if mode != "w" {
                serializer.write_string("mode")?;
                serializer.write_string(mode)?;
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("mode").unwrap();
                    dbg_serializer.write_string(mode).unwrap();
                    dbg_serializer.flush()
                });
            }
        }

        if let Some(db) = db {
            serializer.write_string("db")?;
            serializer.write_string(db)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("db").unwrap();
                dbg_serializer.write_string(db).unwrap();
                dbg_serializer.flush()
            });
        }

        if let Some(imp_user) = imp_user {
            serializer.write_string("imp_user")?;
            serializer.write_string(imp_user)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("imp_user").unwrap();
                dbg_serializer.write_string(imp_user).unwrap();
                dbg_serializer.flush()
            });
        }

        callbacks = callbacks.with_on_success_pre_hook({
            let last_qid = Arc::clone(&self.last_qid);
            move |meta| match meta.get("qid") {
                Some(ValueReceive::Integer(qid)) => {
                    *last_qid.borrow_mut() = Some(*qid);
                    Ok(())
                }
                None => {
                    *last_qid.borrow_mut() = None;
                    Ok(())
                }
                Some(v) => Err(Neo4jError::protocol_error(format!(
                    "server send non-int qid: {:?}",
                    v
                ))),
            }
        });

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Run, callbacks));
        debug_buf_end!(self, log_buf);
        Ok(())
    }

    pub(crate) fn discard(&mut self, n: i64, qid: i64, callbacks: ResponseCallbacks) -> Result<()> {
        self.pull_or_discard(n, qid, callbacks, "DISCARD", 0x2F, ResponseMessage::Discard)
    }

    pub(crate) fn pull(&mut self, n: i64, qid: i64, callbacks: ResponseCallbacks) -> Result<()> {
        self.pull_or_discard(n, qid, callbacks, "PULL", 0x3F, ResponseMessage::Pull)
    }

    fn pull_or_discard(
        &mut self,
        n: i64,
        qid: i64,
        callbacks: ResponseCallbacks,
        name: &str,
        tag: u8,
        response: ResponseMessage,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: {}", name);
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(tag, 1)?;

        let can_omit_qid = self.can_omit_qid(qid);
        let extra_size = 1 + <bool as Into<u64>>::into(!can_omit_qid);
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.write_string("n").unwrap();
            dbg_serializer.write_int(n).unwrap();
            dbg_serializer.flush()
        });
        serializer.write_dict_header(extra_size)?;
        serializer.write_string("n")?;
        serializer.write_int(n)?;
        if !can_omit_qid {
            serializer.write_string("qid")?;
            serializer.write_int(qid)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("qid").unwrap();
                dbg_serializer.write_int(qid).unwrap();
                dbg_serializer.flush()
            });
        }

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::new(response, callbacks));
        debug_buf_end!(self, log_buf);
        Ok(())
    }

    fn can_omit_qid(&self, qid: i64) -> bool {
        qid == -1 || Some(qid) == *(self.last_qid.deref().borrow())
    }

    pub(crate) fn begin<K: Borrow<str> + Debug>(
        &mut self,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_metadata: Option<&HashMap<K, ValueSend>>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: BEGIN");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        let translator = Bolt5x0StructTranslator {};
        serializer.write_struct_header(0x11, 1)?;

        let extra_size = [
            bookmarks.is_some() && !bookmarks.unwrap().is_empty(),
            tx_timeout.is_some(),
            tx_metadata.is_some() && !tx_metadata.unwrap().is_empty(),
            mode.is_some() && mode.unwrap() != "w",
            db.is_some(),
            imp_user.is_some(),
        ]
        .into_iter()
        .map(<bool as Into<u64>>::into)
        .sum();

        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.flush()
        });
        serializer.write_dict_header(extra_size)?;

        if let Some(bookmarks) = bookmarks {
            if !bookmarks.is_empty() {
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("bookmarks").unwrap();
                    self.serialize_str_slice(&mut dbg_serializer, bookmarks)
                        .unwrap();
                    dbg_serializer.flush()
                });
                serializer.write_string("bookmarks").unwrap();
                self.serialize_str_slice(&mut serializer, bookmarks)?;
            }
        }

        if let Some(tx_timeout) = tx_timeout {
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("tx_timeout").unwrap();
                dbg_serializer.write_int(tx_timeout).unwrap();
                dbg_serializer.flush()
            });
            serializer.write_string("tx_timeout")?;
            serializer.write_int(tx_timeout)?;
        }

        if let Some(tx_metadata) = tx_metadata {
            if !tx_metadata.is_empty() {
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("tx_metadata").unwrap();
                    self.serialize_dict(&mut dbg_serializer, &translator, tx_metadata)
                        .unwrap();
                    dbg_serializer.flush()
                });
                serializer.write_string("tx_metadata")?;
                self.serialize_dict(&mut serializer, &translator, tx_metadata)?;
            }
        }

        if let Some(mode) = mode {
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("mode").unwrap();
                dbg_serializer.write_string(mode).unwrap();
                dbg_serializer.flush()
            });
            serializer.write_string("mode")?;
            serializer.write_string(mode)?;
        }

        if let Some(db) = db {
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("db").unwrap();
                dbg_serializer.write_string(db).unwrap();
                dbg_serializer.flush()
            });
            serializer.write_string("db")?;
            serializer.write_string(db)?;
        }

        if let Some(imp_user) = imp_user {
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("imp_user").unwrap();
                dbg_serializer.write_string(imp_user).unwrap();
                dbg_serializer.flush()
            });
            serializer.write_string("imp_user")?;
            serializer.write_string(imp_user)?;
        }

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Begin));
        debug_buf_end!(self, log_buf);
        Ok(())
    }

    pub(crate) fn commit(&mut self, callbacks: ResponseCallbacks) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x12, 0)?;

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Commit, callbacks));
        bolt_debug!(self, "C: COMMIT");
        Ok(())
    }

    pub(crate) fn rollback(&mut self) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x13, 0)?;

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Rollback));
        bolt_debug!(self, "C: ROLLBACK");
        Ok(())
    }

    pub(crate) fn route(
        &mut self,
        routing_context: &HashMap<String, ValueSend>,
        bookmarks: Option<&[String]>,
        db: Option<&str>,
        imp_user: Option<&str>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: ROUTE");
        let translator = Bolt5x0StructTranslator {};
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x66, 3)?;

        debug_buf!(log_buf, " {}", {
            self.serialize_dict(&mut dbg_serializer, &translator, routing_context)
                .unwrap();
            dbg_serializer.flush()
        });
        self.serialize_dict(&mut serializer, &translator, routing_context)?;
        match bookmarks {
            None => {
                debug_buf!(log_buf, " {}", {
                    dbg_serializer.write_list_header(0).unwrap();
                    dbg_serializer.flush()
                });
                serializer.write_list_header(0)?;
            }
            Some(bms) => {
                debug_buf!(log_buf, " {}", {
                    self.serialize_str_slice(&mut dbg_serializer, bms).unwrap();
                    dbg_serializer.flush()
                });
                self.serialize_str_slice(&mut serializer, bms)?;
            }
        }

        let extra_size = <bool as Into<usize>>::into(db.is_some())
            + <bool as Into<usize>>::into(imp_user.is_some());
        serializer.write_dict_header(u64::from_usize(extra_size))?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer
                .write_dict_header(u64::from_usize(extra_size))
                .unwrap();
            dbg_serializer.flush()
        });

        if let Some(db) = db {
            serializer.write_string("db")?;
            serializer.write_string(db)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("db").unwrap();
                dbg_serializer.write_string(db).unwrap();
                dbg_serializer.flush()
            });
        }

        if let Some(imp_user) = imp_user {
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("imp_user").unwrap();
                dbg_serializer.write_string(imp_user).unwrap();
                dbg_serializer.flush()
            });
            serializer.write_string("imp_user")?;
            serializer.write_string(imp_user)?;
        }

        self.message_buff.push_back(vec![message_buff]);
        self.responses
            .push_back(BoltResponse::new(ResponseMessage::Route, callbacks));
        debug_buf_end!(self, log_buf);
        Ok(())
    }

    fn serialize_dict<S: PackStreamSerializer, T: BoltStructTranslator, K: Borrow<str>>(
        &self,
        serializer: &mut S,
        translator: &T,
        map: &HashMap<K, ValueSend>,
    ) -> result::Result<(), S::Error> {
        serializer.write_dict_header(u64::from_usize(map.len()))?;
        for (k, v) in map {
            serializer.write_string(k.borrow())?;
            self.serialize_value(serializer, translator, v)?;
        }
        Ok(())
    }

    fn serialize_str_slice<S: PackStreamSerializer, V: Borrow<str>>(
        &self,
        serializer: &mut S,
        slice: &[V],
    ) -> result::Result<(), S::Error> {
        serializer.write_list_header(u64::from_usize(slice.len()))?;
        for v in slice {
            serializer.write_string(v.borrow())?;
        }
        Ok(())
    }

    fn serialize_value<S: PackStreamSerializer, T: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        translator: &T,
        v: &ValueSend,
    ) -> result::Result<(), S::Error> {
        translator.serialize(serializer, v).map_err(Into::into)
    }

    pub(crate) fn read_all(&mut self, deadline: Option<Instant>) -> Result<()> {
        while self.expects_reply() {
            self.read_one(deadline)?
        }
        Ok(())
    }

    pub(crate) fn read_one(&mut self, deadline: Option<Instant>) -> Result<()> {
        let mut response = self
            .responses
            .pop_front()
            .expect("called Bolt::read_one with empty response queue");

        let mut reader = DeadlineIO::new(
            &mut self.reader,
            &mut self.writer,
            deadline,
            self.socket.as_ref(),
            |err| {
                bolt_debug!(self, "read failed: {}", err);
                self.connection_state = ConnectionState::Broken;
                self.socket.as_ref().map(|s| s.shutdown(Shutdown::Both));
            },
        );
        let mut dechunker = Dechunker::new(&mut reader);
        let translator = Bolt5x0StructTranslator {};
        let message_result: Result<BoltMessage<ValueReceive>> =
            BoltMessage::load(&mut dechunker, |r| {
                let mut deserializer = PackStreamDeserializerImpl::new(r);
                deserializer.load(&translator).map_err(Into::into)
            });
        drop(dechunker);
        let message = reader.rewrite_error(message_result)?;
        match message {
            BoltMessage {
                tag: 0x70,
                mut fields,
            } => {
                // SUCCESS
                Self::assert_response_field_count("SUCCESS", &fields, 1)?;
                let meta = fields.pop().unwrap();
                bolt_debug!(self, "S: SUCCESS {}", meta.dbg_print());
                self.bolt_state.success(response.message, &meta);
                response.callbacks.on_success(meta)
            }
            BoltMessage { tag: 0x7E, fields } => {
                // IGNORED
                Self::assert_response_field_count("IGNORED", &fields, 0)?;
                bolt_debug!(self, "S: IGNORED");
                response.callbacks.on_ignored()
            }
            BoltMessage {
                tag: 0x7F,
                mut fields,
            } => {
                // FAILURE
                Self::assert_response_field_count("FAILURE", &fields, 1)?;
                let meta = fields.pop().unwrap();
                bolt_debug!(self, "S: FAILURE {}", meta.dbg_print());
                self.bolt_state.failure();
                response.callbacks.on_failure(meta)
            }
            BoltMessage {
                tag: 0x71,
                mut fields,
            } => {
                // RECORD
                Self::assert_response_field_count("RECORD", &fields, 1)?;
                let data = fields.pop().unwrap();
                bolt_debug!(self, "S: RECORD [...]");
                let res = response.callbacks.on_record(data);
                self.responses.push_front(response);
                res
            }
            BoltMessage { tag, .. } => Err(Neo4jError::protocol_error(format!(
                "unknown response message tag {:02X?}",
                tag
            ))),
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
            Err(Neo4jError::protocol_error(format!(
                "{} response should have {} field(s) but found {:?}",
                name,
                expected_count,
                fields.len()
            )))
        }
    }

    pub(crate) fn write_all(&mut self, deadline: Option<Instant>) -> Result<()> {
        while self.has_buffered_message() {
            self.write_one(deadline)?
        }
        Ok(())
    }

    pub(crate) fn write_one(&mut self, deadline: Option<Instant>) -> Result<()> {
        if let Some(message_buff) = self.message_buff.pop_front() {
            let chunker = Chunker::new(&message_buff);
            let mut writer = DeadlineIO::new(
                &mut self.reader,
                &mut self.writer,
                deadline,
                self.socket.as_ref(),
                |err| {
                    bolt_debug!(self, "write failed: {}", err);
                    self.connection_state = ConnectionState::Broken;
                    self.socket.as_ref().map(|s| s.shutdown(Shutdown::Both));
                },
            );
            for chunk in chunker {
                let res = Neo4jError::wrap_write(writer.write_all(&chunk));
                writer.rewrite_error(res)?;
            }
            let res = Neo4jError::wrap_write(writer.flush());
            writer.rewrite_error(res)?;
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
        !(self.bolt_state.state() == BoltState::Ready && self.responses.is_empty())
    }
}

impl<R: Read, W: Write> Debug for Bolt<R, W> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Bolt{}x{} {{\n  message_buff: {:?}\n  responses: {:?}\n}}",
            self.version.0, self.version.1, self.message_buff, self.responses
        )
    }
}

impl<R: Read, W: Write> Drop for Bolt<R, W> {
    fn drop(&mut self) {
        if self.closed() {
            return;
        }
        self.message_buff.clear();
        self.responses.clear();
        if self.goodbye().is_err() {
            return;
        }
        let _ = self.write_all(Some(Instant::now() + Duration::from_millis(100)));
    }
}

pub(crate) trait BoltStructTranslator {
    fn serialize<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        value: &ValueSend,
    ) -> result::Result<(), S::Error>;

    fn deserialize_struct(&self, tag: u8, fields: Vec<ValueReceive>) -> ValueReceive;
}

const BOLT_MAGIC_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const BOLT_VERSION_OFFER: [u8; 16] = [
    0, 0, 0, 5, // BOLT 5.0
    0, 0, 4, 4, // BOLT 4.4
    0, 0, 0, 0, // -
    0, 0, 0, 0, // -
];

pub(crate) fn open(
    address: Arc<Address>,
    deadline: Option<Instant>,
    mut connect_timeout: Option<Duration>,
) -> Result<TcpBolt> {
    debug!(
        "{}{}",
        dbg_extra(None, None),
        format!("C: <OPEN> {}", address)
    );
    if let Some(deadline) = deadline {
        let mut time_left = deadline.saturating_duration_since(Instant::now());
        if time_left == Duration::from_secs(0) {
            time_left = Duration::from_nanos(1);
        }
        match connect_timeout {
            None => connect_timeout = Some(time_left),
            Some(timeout) => connect_timeout = Some(timeout.min(time_left)),
        }
    }
    let stream = Neo4jError::wrap_connect(match connect_timeout {
        None => TcpStream::connect(&*address),
        Some(timeout) => each_addr(&*address, |addr| TcpStream::connect_timeout(addr?, timeout)),
    })?;
    let local_port = stream
        .local_addr()
        .map(|addr| addr.port())
        .unwrap_or_default();

    // TODO: TLS

    let mut reader = BufReader::new(Neo4jError::wrap_connect(stream.try_clone())?);
    let mut writer = BufWriter::new(Neo4jError::wrap_connect(stream.try_clone())?);
    let mut deadline_io =
        DeadlineIO::new(&mut reader, &mut writer, deadline, Some(&stream), |err| {
            socket_debug!(local_port, "io failure: {err}");
            let _ = stream.shutdown(Shutdown::Both);
        });

    socket_debug!(local_port, "C: <HANDSHAKE> {:02X?}", BOLT_MAGIC_PREAMBLE);
    wrap_write_socket(local_port, deadline_io.write_all(&BOLT_MAGIC_PREAMBLE))?;
    socket_debug!(local_port, "C: <BOLT> {:02X?}", BOLT_VERSION_OFFER);
    wrap_write_socket(local_port, deadline_io.write_all(&BOLT_VERSION_OFFER))?;
    wrap_write_socket(local_port, deadline_io.flush())?;

    let mut negotiated_version = [0u8; 4];
    wrap_read_socket(local_port, deadline_io.read_exact(&mut negotiated_version))?;
    socket_debug!(local_port, "S: <BOLT> {:02X?}", negotiated_version);

    let version = match negotiated_version {
        [0, 0, 0, 0] => Err(Neo4jError::InvalidConfig {
            message: String::from("Server version not supported."),
        }),
        [0, 0, 0, 5] => Ok((5, 0)),
        [0, 0, 4, 4] => Ok((4, 4)),
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

    Ok(Bolt::new(
        version,
        reader,
        writer,
        Some(stream),
        Some(local_port),
        address,
    ))
}

// copied from std::net
fn each_addr<A: ToSocketAddrs, F, T>(addr: A, mut f: F) -> io::Result<T>
where
    F: FnMut(io::Result<&SocketAddr>) -> io::Result<T>,
{
    let addrs = match addr.to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(e) => return f(Err(e)),
    };
    let mut last_err = None;
    for addr in addrs {
        match f(Ok(&addr)) {
            Ok(l) => return Ok(l),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any addresses",
        )
    }))
}

fn wrap_write_socket<T>(local_port: u16, res: io::Result<T>) -> Result<T> {
    match res {
        Ok(res) => Ok(res),
        Err(err) => {
            socket_debug!(local_port, "   write error: {}", err);
            Neo4jError::wrap_write(Err(err))
        }
    }
}

fn wrap_read_socket<T>(local_port: u16, res: io::Result<T>) -> Result<T> {
    match res {
        Ok(res) => Ok(res),
        Err(err) => {
            socket_debug!(local_port, "   read error: {}", err);
            Neo4jError::wrap_read(Err(err))
        }
    }
}
