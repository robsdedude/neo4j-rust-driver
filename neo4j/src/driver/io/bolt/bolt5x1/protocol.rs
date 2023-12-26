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
use std::fmt::Debug;
use std::io::{Error as IoError, Read, Write};
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, log_enabled, warn, Level};
use usize_cast::FromUsize;

use super::super::bolt5x0::Bolt5x0;
use super::super::message::BoltMessage;
use super::super::message_parameters::{
    BeginParameters, CommitParameters, DiscardParameters, GoodbyeParameters, HelloParameters,
    PullParameters, ReauthParameters, ResetParameters, RollbackParameters, RouteParameters,
    RunParameters,
};
use super::super::packstream::{
    PackStreamSerializer, PackStreamSerializerDebugImpl, PackStreamSerializerImpl,
};
use super::super::{
    bolt_debug, bolt_debug_extra, dbg_extra, debug_buf, debug_buf_end, debug_buf_start, BoltData,
    BoltProtocol, BoltResponse, BoltStructTranslator, OnServerErrorCb, ResponseCallbacks,
    ResponseMessage,
};
use crate::error_::Result;
use crate::value::ValueReceive;

const SERVER_AGENT_KEY: &str = "server";
const HINTS_KEY: &str = "hints";
const RECV_TIMEOUT_KEY: &str = "connection.recv_timeout_seconds";

#[derive(Debug, Default)]
pub(crate) struct Bolt5x1<T: BoltStructTranslator> {
    translator: T,
    bolt5x0: Bolt5x0<T>,
}

impl<T: BoltStructTranslator> Bolt5x1<T> {
    fn logon<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ReauthParameters,
    ) -> Result<()> {
        let ReauthParameters {
            auth,
            session_auth: _,
        } = parameters;

        data.auth = Some(Arc::clone(auth));
        let auth = data
            .auth
            .as_ref()
            .expect("is some because of previous line");

        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: LOGON");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x6A, 1)?;

        let auth_size = u64::from_usize(auth.data.len());
        serializer.write_dict_header(auth_size)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(auth_size).unwrap();
            dbg_serializer.flush()
        });

        for (k, v) in &auth.data {
            serializer.write_string(k)?;
            data.serialize_value(&mut serializer, &self.translator, v)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string(k).unwrap();
                if k == "credentials" {
                    dbg_serializer.write_string("**********").unwrap();
                } else {
                    data.serialize_value(&mut dbg_serializer, &self.translator, v)
                        .unwrap();
                }
                dbg_serializer.flush()
            });
        }

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Logon));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    fn logoff<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()> {
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x6B, 0)?;
        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Logoff));
        bolt_debug!(data, "C: LOGOFF");
        Ok(())
    }
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x1<T> {
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: HelloParameters,
    ) -> Result<()> {
        let HelloParameters {
            user_agent,
            auth: _,
            routing_context,
        } = parameters;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x01, 1)?;

        let extra_size = 1 + <bool as Into<u64>>::into(routing_context.is_some());
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
            data.serialize_routing_context(&mut serializer, &self.translator, routing_context)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("routing").unwrap();
                data.serialize_routing_context(
                    &mut dbg_serializer,
                    &self.translator,
                    routing_context,
                )
                .unwrap();
                dbg_serializer.flush()
            });
        }

        data.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(data, log_buf);

        let bolt_meta = Arc::clone(&data.meta);
        let bolt_server_agent = Arc::clone(&data.server_agent);
        let socket = Arc::clone(&data.socket);
        data.responses.push_back(BoltResponse::new(
            ResponseMessage::Hello,
            ResponseCallbacks::new().with_on_success(move |mut meta| {
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
                if let Some(value) = meta.get(HINTS_KEY) {
                    match value {
                        ValueReceive::Map(value) => {
                            if let Some(timeout) = value.get(RECV_TIMEOUT_KEY) {
                                match timeout {
                                    ValueReceive::Integer(timeout) if timeout > &0 => {
                                        socket.deref().as_ref().map(|socket| {
                                            let timeout = Some(Duration::from_secs(*timeout as u64));
                                            socket.set_read_timeout(timeout)?;
                                            socket.set_write_timeout(timeout)?;
                                            Ok(())
                                        }).transpose().unwrap_or_else(|err: IoError| {
                                            warn!("Failed to set socket timeout as hinted by the server: {err}");
                                            None
                                        });
                                    }
                                    ValueReceive::Integer(_) => {
                                        warn!(
                                            "Server sent unexpected {RECV_TIMEOUT_KEY} value {:?}",
                                            timeout
                                        );
                                    }
                                    _ => {
                                        warn!(
                                            "Server sent unexpected {RECV_TIMEOUT_KEY} type {:?}",
                                            timeout
                                        );
                                    }
                                }
                            }
                        }
                        _ => {
                            warn!("Server sent unexpected {HINTS_KEY} type {:?}", value);
                        }
                    }
                }
                mem::swap(&mut *bolt_meta.borrow_mut(), &mut meta);
                Ok(())
            }),
        ));
        Ok(())
    }

    #[inline]
    fn reauth<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ReauthParameters,
    ) -> Result<()> {
        if data.auth.is_some() {
            self.logoff(data)?;
        }
        data.auth = Some(Arc::clone(parameters.auth));
        self.logon(data, parameters)
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        true
    }

    #[inline]
    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: GoodbyeParameters,
    ) -> Result<()> {
        self.bolt5x0.goodbye(data, parameters)
    }

    #[inline]
    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ResetParameters,
    ) -> Result<()> {
        self.bolt5x0.reset(data, parameters)
    }

    #[inline]
    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.run(data, parameters, callbacks)
    }
    #[inline]
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.discard(data, parameters, callbacks)
    }

    #[inline]
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.pull(data, parameters, callbacks)
    }

    #[inline]
    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
    ) -> Result<()> {
        self.bolt5x0.begin(data, parameters)
    }

    #[inline]
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.commit(data, parameters, callbacks)
    }

    #[inline]
    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RollbackParameters,
    ) -> Result<()> {
        self.bolt5x0.rollback(data, parameters)
    }

    #[inline]
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.route(data, parameters, callbacks)
    }

    #[inline]
    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x0.load_value(reader)
    }

    #[inline]
    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        self.bolt5x0
            .handle_response(bolt_data, message, on_server_error)
    }
}
