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
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::{debug, log_enabled, warn, Level};
use usize_cast::FromUsize;

use super::super::bolt5x0::Bolt5x0;
use super::super::message::BoltMessage;
use super::super::message_parameters::RunParameters;
use super::super::packstream::{
    PackStreamSerializer, PackStreamSerializerDebugImpl, PackStreamSerializerImpl,
};
use super::super::{
    bolt_debug_extra, dbg_extra, debug_buf, debug_buf_end, debug_buf_start, BoltData, BoltProtocol,
    BoltResponse, BoltStructTranslatorWithUtcPatch, OnServerErrorCb, ResponseCallbacks,
    ResponseMessage,
};
use crate::{Result, ValueReceive, ValueSend};

const SERVER_AGENT_KEY: &str = "server";
const PATCH_BOLT_KEY: &str = "patch_bolt";

#[derive(Debug)]
pub(crate) struct Bolt4x4<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> {
    translator: Arc<AtomicRefCell<T>>,
    bolt5x0: Bolt5x0<Arc<AtomicRefCell<T>>>,
}

impl<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> Bolt4x4<T> {
    pub(crate) fn new() -> Self {
        let translator: Arc<AtomicRefCell<T>> = Default::default();
        let bolt5x0 = Bolt5x0::new(Arc::clone(&translator));
        Bolt4x4 {
            translator,
            bolt5x0,
        }
    }
}

impl<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> Default for Bolt4x4<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> BoltProtocol for Bolt4x4<T> {
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        user_agent: &str,
        auth: &HashMap<String, ValueSend>,
        routing_context: Option<&HashMap<String, ValueSend>>,
    ) -> Result<()> {
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO");
        let translator = &*(*self.translator).borrow();
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x01, 1)?;

        let extra_size =
            2 + <bool as Into<u64>>::into(routing_context.is_some()) + u64::from_usize(auth.len());
        serializer.write_dict_header(extra_size)?;
        serializer.write_string("user_agent")?;
        serializer.write_string(user_agent)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.write_string("user_agent").unwrap();
            dbg_serializer.write_string(user_agent).unwrap();
            dbg_serializer.flush()
        });

        serializer.write_string("patch_bolt")?;
        data.serialize_str_slice(&mut serializer, &["utc"])?;
        debug_buf!(log_buf, "{}", {
            dbg_serializer.write_string("patch_bolt").unwrap();
            data.serialize_str_slice(&mut dbg_serializer, &["utc"])
                .unwrap();
            dbg_serializer.flush()
        });

        if let Some(routing_context) = routing_context {
            serializer.write_string("routing")?;
            data.serialize_routing_context(&mut serializer, translator, routing_context)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string("routing").unwrap();
                data.serialize_routing_context(&mut dbg_serializer, translator, routing_context)
                    .unwrap();
                dbg_serializer.flush()
            });
        }

        for (k, v) in auth {
            serializer.write_string(k)?;
            data.serialize_value(&mut serializer, translator, v)?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer.write_string(k).unwrap();
                if k == "credentials" {
                    dbg_serializer.write_string("**********").unwrap();
                } else {
                    data.serialize_value(&mut dbg_serializer, translator, v)
                        .unwrap();
                }
                dbg_serializer.flush()
            });
        }

        data.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(data, log_buf);

        let bolt_meta = Arc::clone(&data.meta);
        let translator = Arc::clone(&self.translator);
        let bolt_server_agent = Arc::clone(&data.server_agent);
        data.responses.push_back(BoltResponse::new(
            ResponseMessage::Hello,
            ResponseCallbacks::new().with_on_success(move |mut meta| {
                if let Some((key, value)) = meta.remove_entry(SERVER_AGENT_KEY) {
                    match value {
                        ValueReceive::String(value) => {
                            mem::swap(&mut *bolt_server_agent.borrow_mut(), &mut Arc::new(value));
                        }
                        _ => {
                            warn!(
                                "Server sent unexpected {SERVER_AGENT_KEY} type {:?}",
                                &value
                            );
                            meta.insert(key, value);
                        }
                    }
                }
                if let Some(value) = meta.get(PATCH_BOLT_KEY) {
                    match value {
                        ValueReceive::List(value) => {
                            for entry in value {
                                match entry {
                                    ValueReceive::String(s) if s == "utc" => {
                                        translator.borrow_mut().enable_utc_patch();
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {
                            warn!("Server sent unexpected {PATCH_BOLT_KEY} type {:?}", value);
                        }
                    }
                }
                mem::swap(&mut *bolt_meta.borrow_mut(), &mut meta);
                Ok(())
            }),
        ));
        Ok(())
    }

    fn goodbye<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()> {
        self.bolt5x0.goodbye(data)
    }

    fn reset<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()> {
        self.bolt5x0.reset(data)
    }

    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.run(data, parameters, callbacks)
    }

    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        n: i64,
        qid: i64,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.discard(data, n, qid, callbacks)
    }

    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        n: i64,
        qid: i64,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.pull(data, n, qid, callbacks)
    }

    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_metadata: Option<&HashMap<K, ValueSend>>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<()> {
        self.bolt5x0
            .begin(data, bookmarks, tx_timeout, tx_metadata, mode, db, imp_user)
    }

    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0.commit(data, callbacks)
    }

    fn rollback<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()> {
        self.bolt5x0.rollback(data)
    }

    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        routing_context: &HashMap<String, ValueSend>,
        bookmarks: Option<&[String]>,
        db: Option<&str>,
        imp_user: Option<&str>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x0
            .route(data, routing_context, bookmarks, db, imp_user, callbacks)
    }

    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x0.load_value(reader)
    }

    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb,
    ) -> Result<()> {
        self.bolt5x0
            .handle_response(bolt_data, message, on_server_error)
    }
}
