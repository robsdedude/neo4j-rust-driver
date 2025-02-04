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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::mem;
use std::net::TcpStream;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use atomic_refcell::AtomicRefCell;
use log::{debug, log_enabled, warn, Level};
use usize_cast::FromUsize;

use super::super::bolt_common::{unsupported_protocol_feature_error, ServerAwareBoltVersion};
use super::super::message::BoltMessage;
use super::super::message_parameters::{
    BeginParameters, CommitParameters, DiscardParameters, GoodbyeParameters, HelloParameters,
    PullParameters, ReauthParameters, ResetParameters, RollbackParameters, RouteParameters,
    RunParameters, TelemetryParameters,
};
use super::super::packstream::{
    PackStreamDeserializer, PackStreamDeserializerImpl, PackStreamSerializer,
    PackStreamSerializerDebugImpl, PackStreamSerializerImpl,
};
use super::super::{
    assert_response_field_count, bolt_debug, bolt_debug_extra, dbg_extra, debug_buf, debug_buf_end,
    debug_buf_start, BoltData, BoltMeta, BoltProtocol, BoltResponse, BoltStructTranslator,
    ConnectionState, OnServerErrorCb, ResponseCallbacks, ResponseMessage,
};
use crate::driver::config::auth::AuthToken;
use crate::driver::config::notification::NotificationFilter;
use crate::driver::session::bookmarks::Bookmarks;
use crate::error_::{Neo4jError, Result, ServerError};
use crate::value::{ValueReceive, ValueSend};

const SERVER_AGENT_KEY: &str = "server";
const HINTS_KEY: &str = "hints";
const RECV_TIMEOUT_KEY: &str = "connection.recv_timeout_seconds";

#[derive(Debug)]
pub(crate) struct Bolt5x0<T: BoltStructTranslator> {
    translator: T,
    protocol_version: ServerAwareBoltVersion,
}

impl<T: BoltStructTranslator> Bolt5x0<T> {
    pub(in super::super) fn new(translator: T, protocol_version: ServerAwareBoltVersion) -> Self {
        Bolt5x0 {
            translator,
            protocol_version,
        }
    }

    pub(in super::super) fn try_parse_error(meta: ValueReceive) -> Result<ServerError> {
        let meta = meta
            .try_into_map()
            .map_err(|_| Neo4jError::protocol_error("FAILURE meta was not a Dictionary"))?;
        Ok(ServerError::from_meta(meta))
    }
}

impl<T: BoltStructTranslator> Default for Bolt5x0<T> {
    fn default() -> Self {
        Self::new(T::default(), ServerAwareBoltVersion::V5x0)
    }
}

impl<T: BoltStructTranslator> Bolt5x0<T> {
    pub(in super::super) fn pull_or_discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        n: i64,
        qid: i64,
        callbacks: ResponseCallbacks,
        message: PullOrDiscardMessageSpec,
    ) -> Result<()> {
        let PullOrDiscardMessageSpec {
            name,
            tag,
            response,
        } = message;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: {}", name);
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(tag, 1)?;

        let can_omit_qid = data.can_omit_qid(qid);
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

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::new(response, callbacks));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    pub(crate) fn check_no_notification_filter(
        &self,
        notification_filter: Option<&NotificationFilter>,
    ) -> Result<()> {
        if !notification_filter.map(|n| n.is_default()).unwrap_or(true) {
            return Err(unsupported_protocol_feature_error(
                "notification filtering",
                self.protocol_version,
                ServerAwareBoltVersion::V5x2,
            ));
        }
        Ok(())
    }

    pub(in super::super) fn write_str_entry(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        key: &str,
        value: &str,
    ) -> Result<()> {
        serializer.write_string(key)?;
        serializer.write_string(value)?;
        debug_buf!(log_buf, "{}", {
            dbg_serializer.write_string(key).unwrap();
            dbg_serializer.write_string(value).unwrap();
            dbg_serializer.flush()
        });
        Ok(())
    }

    pub(in super::super) fn write_int_entry(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        key: &str,
        value: i64,
    ) -> Result<()> {
        serializer.write_string(key)?;
        serializer.write_int(value)?;
        debug_buf!(log_buf, "{}", {
            dbg_serializer.write_string(key).unwrap();
            dbg_serializer.write_int(value).unwrap();
            dbg_serializer.flush()
        });
        Ok(())
    }

    pub(in super::super) fn write_dict_entry(
        &self,
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        key: &str,
        value: &HashMap<impl Borrow<str> + Debug, ValueSend>,
    ) -> Result<()> {
        serializer.write_string(key)?;
        data.serialize_dict(serializer, &self.translator, value)?;
        debug_buf!(log_buf, "{}", {
            dbg_serializer.write_string(key).unwrap();
            data.serialize_dict(dbg_serializer, &self.translator, value)
                .unwrap();
            dbg_serializer.flush()
        });
        Ok(())
    }

    pub(in super::super) fn write_user_agent_entry(
        log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        user_agent: &str,
    ) -> Result<()> {
        Self::write_str_entry(
            log_buf,
            serializer,
            dbg_serializer,
            "user_agent",
            user_agent,
        )
    }

    pub(in super::super) fn write_routing_context_entry(
        &self,
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        routing_context: Option<&HashMap<String, ValueSend>>,
    ) -> Result<()> {
        if let Some(routing_context) = routing_context {
            serializer.write_string("routing")?;
            data.serialize_dict(serializer, &self.translator, routing_context)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("routing").unwrap();
                data.serialize_dict(dbg_serializer, &self.translator, routing_context)
                    .unwrap();
                dbg_serializer.flush()
            });
        }
        Ok(())
    }

    pub(in super::super) fn write_auth_entries(
        &self,
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        auth: &Arc<AuthToken>,
    ) -> Result<()> {
        for (k, v) in &auth.data {
            serializer.write_string(k)?;
            data.serialize_value(serializer, &self.translator, v)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string(k).unwrap();
                if k == "credentials" {
                    dbg_serializer.write_string("**********").unwrap();
                } else {
                    data.serialize_value(dbg_serializer, &self.translator, v)
                        .unwrap();
                }
                dbg_serializer.flush()
            });
        }
        Ok(())
    }

    pub(in super::super) fn hello_response_handle_agent(
        meta: &mut BoltMeta,
        bolt_server_agent: &AtomicRefCell<Arc<String>>,
    ) {
        if let Some((key, value)) = meta.remove_entry(SERVER_AGENT_KEY) {
            match value {
                ValueReceive::String(value) => {
                    mem::swap(&mut *bolt_server_agent.borrow_mut(), &mut Arc::new(value));
                }
                _ => {
                    warn!("Server sent unexpected server_agent type {:?}", &value);
                    meta.insert(key, value);
                }
            }
        }
    }

    pub(in super::super) fn hello_response_handle_timeout_hint(
        hints: &HashMap<String, ValueReceive>,
        socket: Option<&TcpStream>,
    ) {
        if let Some(timeout) = hints.get(RECV_TIMEOUT_KEY) {
            match timeout {
                ValueReceive::Integer(timeout) if timeout > &0 => {
                    socket
                        .map(|socket| {
                            let timeout = Some(Duration::from_secs(*timeout as u64));
                            socket.set_read_timeout(timeout)
                        })
                        .transpose()
                        .unwrap_or_else(|err| {
                            warn!("Failed to set socket timeout as hinted by the server: {err}");
                            None
                        });
                }
                ValueReceive::Integer(_) => {
                    warn!("Server sent unexpected {RECV_TIMEOUT_KEY} value {timeout:?}");
                }
                _ => {
                    warn!("Server sent unexpected {RECV_TIMEOUT_KEY} type {timeout:?}");
                }
            }
        }
    }

    pub(in super::super) fn hello_response_handle_connection_hints(
        meta: &BoltMeta,
        socket: Option<&TcpStream>,
    ) {
        if let Some(value) = meta.get(HINTS_KEY) {
            match value {
                ValueReceive::Map(value) => {
                    Self::hello_response_handle_timeout_hint(value, socket);
                }
                _ => {
                    warn!("Server sent unexpected {HINTS_KEY} type {:?}", value);
                }
            }
        }
    }

    pub(in super::super) fn enqueue_hello_response(data: &mut BoltData<impl Read + Write>) {
        let bolt_meta = Arc::clone(&data.meta);
        let bolt_server_agent = Arc::clone(&data.server_agent);
        let socket = Arc::clone(&data.socket);

        data.responses.push_back(BoltResponse::new(
            ResponseMessage::Hello,
            ResponseCallbacks::new().with_on_success(move |mut meta| {
                Self::hello_response_handle_agent(&mut meta, &bolt_server_agent);
                Self::hello_response_handle_connection_hints(&meta, socket.deref().as_ref());
                mem::swap(&mut *bolt_meta.borrow_mut(), &mut meta);
                Ok(())
            }),
        ));
    }

    pub(in super::super) fn write_parameter_dict(
        &self,
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        parameters: Option<&HashMap<impl Borrow<str> + Debug, ValueSend>>,
    ) -> Result<()> {
        match parameters {
            Some(parameters) => {
                data.serialize_dict(serializer, &self.translator, parameters)?;
                debug_buf!(log_buf, " {}", {
                    data.serialize_dict(dbg_serializer, &self.translator, parameters)
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
        Ok(())
    }

    pub(in super::super) fn write_bookmarks_entry(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        bookmarks: Option<&Bookmarks>,
    ) -> Result<()> {
        if let Some(bookmarks) = bookmarks {
            if !bookmarks.is_empty() {
                serializer.write_string("bookmarks")?;
                data.serialize_str_iter(serializer, bookmarks.raw())?;
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("bookmarks").unwrap();
                    data.serialize_str_iter(dbg_serializer, bookmarks.raw())
                        .unwrap();
                    dbg_serializer.flush()
                });
            }
        }
        Ok(())
    }

    pub(in super::super) fn write_bookmarks_list(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        bookmarks: Option<&Bookmarks>,
    ) -> Result<()> {
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
                    data.serialize_str_iter(dbg_serializer, bms.raw()).unwrap();
                    dbg_serializer.flush()
                });
                data.serialize_str_iter(serializer, bms.raw())?;
            }
        }
        Ok(())
    }

    pub(in super::super) fn write_tx_timeout_entry(
        log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        tx_timeout: Option<i64>,
    ) -> Result<()> {
        if let Some(tx_timeout) = tx_timeout {
            Self::write_int_entry(
                log_buf,
                serializer,
                dbg_serializer,
                "tx_timeout",
                tx_timeout,
            )?;
        }
        Ok(())
    }

    pub(in super::super) fn write_tx_metadata_entry(
        &self,
        log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
        tx_metadata: Option<&HashMap<impl Borrow<str> + Debug, ValueSend>>,
    ) -> Result<()> {
        if let Some(tx_metadata) = tx_metadata {
            if !tx_metadata.is_empty() {
                self.write_dict_entry(
                    log_buf,
                    serializer,
                    dbg_serializer,
                    data,
                    "tx_metadata",
                    tx_metadata,
                )?;
            }
        }
        Ok(())
    }

    pub(in super::super) fn write_mode_entry(
        log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        mode: Option<&str>,
    ) -> Result<()> {
        if let Some(mode) = mode {
            if mode != "w" {
                Self::write_str_entry(log_buf, serializer, dbg_serializer, "mode", mode)?;
            }
        }
        Ok(())
    }

    pub(in super::super) fn write_db_entry(
        log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        db: Option<&str>,
    ) -> Result<()> {
        if let Some(db) = db {
            Self::write_str_entry(log_buf, serializer, dbg_serializer, "db", db)?;
        }
        Ok(())
    }

    pub(in super::super) fn write_imp_user_entry(
        log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        imp_user: Option<&str>,
    ) -> Result<()> {
        if let Some(imp_user) = imp_user {
            Self::write_str_entry(log_buf, serializer, dbg_serializer, "imp_user", imp_user)?;
        }
        Ok(())
    }

    pub(in super::super) fn install_qid_hook(
        data: &BoltData<impl Read + Write>,
        callbacks: ResponseCallbacks,
    ) -> ResponseCallbacks {
        callbacks.with_on_success_pre_hook({
            let last_qid = Arc::clone(&data.last_qid);
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
        })
    }
}

pub(in super::super) struct PullOrDiscardMessageSpec {
    name: &'static str,
    tag: u8,
    response: ResponseMessage,
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x0<T> {
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: HelloParameters,
    ) -> Result<()> {
        let HelloParameters {
            user_agent,
            auth,
            routing_context,
            notification_filter,
        } = parameters;
        self.check_no_notification_filter(Some(notification_filter))?;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x01, 1)?;

        let extra_size = 1
            + <bool as Into<u64>>::into(routing_context.is_some())
            + u64::from_usize(auth.data.len());
        serializer.write_dict_header(extra_size)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.flush()
        });

        Self::write_user_agent_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            user_agent,
        )?;

        self.write_routing_context_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            routing_context,
        )?;

        self.write_auth_entries(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            auth,
        )?;
        data.auth = Some(Arc::clone(auth));

        data.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(data, log_buf);

        Self::enqueue_hello_response(data);
        Ok(())
    }

    #[inline]
    fn reauth<RW: Read + Write>(
        &mut self,
        _: &mut BoltData<RW>,
        _: ReauthParameters,
    ) -> Result<()> {
        Err(unsupported_protocol_feature_error(
            "session authentication",
            self.protocol_version,
            ServerAwareBoltVersion::V5x1,
        ))
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        false
    }

    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        _: GoodbyeParameters,
    ) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x02, 0)?;
        data.message_buff.push_back(vec![message_buff]);
        data.connection_state = ConnectionState::Closed;
        bolt_debug!(data, "C: GOODBYE");
        Ok(())
    }

    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        _: ResetParameters,
    ) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x0F, 0)?;
        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Reset));
        bolt_debug!(data, "C: RESET");
        Ok(())
    }

    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
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
            notification_filter,
        } = parameters;
        self.check_no_notification_filter(notification_filter)?;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: RUN");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x10, 3)?;

        serializer.write_string(query)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_string(query).unwrap();
            dbg_serializer.flush()
        });

        self.write_parameter_dict(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            parameters,
        )?;

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

        Self::write_bookmarks_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            bookmarks,
        )?;

        Self::write_tx_timeout_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            tx_timeout,
        )?;

        self.write_tx_metadata_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            tx_metadata,
        )?;

        Self::write_mode_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, mode)?;

        Self::write_db_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, db)?;

        Self::write_imp_user_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            imp_user,
        )?;

        callbacks = Self::install_qid_hook(data, callbacks);

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::new(ResponseMessage::Run, callbacks));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let DiscardParameters { n, qid } = parameters;
        self.pull_or_discard(
            data,
            n,
            qid,
            callbacks,
            PullOrDiscardMessageSpec {
                name: "DISCARD",
                tag: 0x2F,
                response: ResponseMessage::Discard,
            },
        )
    }

    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let PullParameters { n, qid } = parameters;
        self.pull_or_discard(
            data,
            n,
            qid,
            callbacks,
            PullOrDiscardMessageSpec {
                name: "PULL",
                tag: 0x3F,
                response: ResponseMessage::Pull,
            },
        )
    }

    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let BeginParameters {
            bookmarks,
            tx_timeout,
            tx_metadata,
            mode,
            db,
            imp_user,
            notification_filter,
        } = parameters;
        self.check_no_notification_filter(Some(notification_filter))?;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: BEGIN");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x11, 1)?;

        let extra_size = [
            bookmarks.is_some() && !bookmarks.unwrap().is_empty(),
            tx_timeout.is_some(),
            tx_metadata.map(|m| !m.is_empty()).unwrap_or_default(),
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

        Self::write_bookmarks_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            bookmarks,
        )?;

        Self::write_tx_timeout_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            tx_timeout,
        )?;

        self.write_tx_metadata_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            tx_metadata,
        )?;

        Self::write_mode_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, mode)?;

        Self::write_db_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, db)?;

        Self::write_imp_user_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            imp_user,
        )?;

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::new(ResponseMessage::Begin, callbacks));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        _: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x12, 0)?;

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::new(ResponseMessage::Commit, callbacks));
        bolt_debug!(data, "C: COMMIT");
        Ok(())
    }

    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        _: RollbackParameters,
    ) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x13, 0)?;

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Rollback));
        bolt_debug!(data, "C: ROLLBACK");
        Ok(())
    }

    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let RouteParameters {
            routing_context,
            bookmarks,
            db,
            imp_user,
        } = parameters;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: ROUTE");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x66, 3)?;

        data.serialize_dict(&mut serializer, &self.translator, routing_context)?;
        debug_buf!(log_buf, " {}", {
            data.serialize_dict(&mut dbg_serializer, &self.translator, routing_context)
                .unwrap();
            dbg_serializer.flush()
        });

        Self::write_bookmarks_list(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            bookmarks,
        )?;

        let extra_size = <bool as Into<usize>>::into(db.is_some())
            + <bool as Into<usize>>::into(imp_user.is_some());
        serializer.write_dict_header(u64::from_usize(extra_size))?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer
                .write_dict_header(u64::from_usize(extra_size))
                .unwrap();
            dbg_serializer.flush()
        });

        Self::write_db_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, db)?;

        Self::write_imp_user_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            imp_user,
        )?;

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::new(ResponseMessage::Route, callbacks));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    #[inline]
    fn telemetry<RW: Read + Write>(
        &mut self,
        _data: &mut BoltData<RW>,
        _parameters: TelemetryParameters,
        _callbacks: ResponseCallbacks,
    ) -> Result<()> {
        // TELEMETRY not support by this protocol version, so we ignore it.
        Ok(())
    }

    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        let mut deserializer = PackStreamDeserializerImpl::new(reader);
        deserializer.load(&self.translator).map_err(Into::into)
    }

    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        let mut response = bolt_data
            .responses
            .pop_front()
            .expect("called Bolt::read_one with empty response queue");
        match message {
            BoltMessage {
                tag: 0x70,
                mut fields,
            } => {
                // SUCCESS
                assert_response_field_count("SUCCESS", &fields, 1)?;
                let meta = fields.pop().unwrap();
                bolt_debug!(bolt_data, "S: SUCCESS {}", meta.dbg_print());
                bolt_data.bolt_state.success(
                    response.message,
                    &meta,
                    bolt_data.local_port,
                    bolt_data.meta.try_borrow().as_deref(),
                );
                response.callbacks.on_success(meta)
            }
            BoltMessage { tag: 0x7E, fields } => {
                // IGNORED
                assert_response_field_count("IGNORED", &fields, 0)?;
                bolt_debug!(bolt_data, "S: IGNORED");
                response.callbacks.on_ignored()
            }
            BoltMessage {
                tag: 0x7F,
                mut fields,
            } => {
                // FAILURE
                assert_response_field_count("FAILURE", &fields, 1)?;
                let meta = fields.pop().unwrap();
                bolt_debug!(bolt_data, "S: FAILURE {}", meta.dbg_print());
                let mut error = Self::try_parse_error(meta)?;
                bolt_data.bolt_state.failure();
                match on_server_error {
                    None => response.callbacks.on_failure(error),
                    Some(cb) => {
                        let res1 = cb(bolt_data, &mut error);
                        let res2 = response.callbacks.on_failure(error);
                        match res1 {
                            Ok(()) => res2,
                            Err(e1) => {
                                if let Err(e2) = res2 {
                                    warn!(
                                        "server error swallowed because of user callback error: {e2}"
                                    );
                                }
                                Err(e1)
                            }
                        }
                    }
                }
            }
            BoltMessage {
                tag: 0x71,
                mut fields,
            } => {
                // RECORD
                assert_response_field_count("RECORD", &fields, 1)?;
                let data = fields.pop().unwrap();
                bolt_debug!(bolt_data, "S: RECORD [...]");
                let res = response.callbacks.on_record(data);
                bolt_data.responses.push_front(response);
                res
            }
            BoltMessage { tag, .. } => Err(Neo4jError::protocol_error(format!(
                "unknown response message tag {:02X?}",
                tag
            ))),
        }
    }
}
