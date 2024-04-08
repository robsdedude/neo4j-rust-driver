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
use std::ops::Deref;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use log::{debug, log_enabled, warn, Level};
use usize_cast::FromUsize;

use super::super::bolt5x0::Bolt5x0;
use super::super::bolt_common::{unsupported_protocol_feature_error, ServerAwareBoltVersion};
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
    bolt_debug_extra, dbg_extra, debug_buf, debug_buf_end, debug_buf_start, BoltData, BoltProtocol,
    BoltResponse, BoltStructTranslatorWithUtcPatch, OnServerErrorCb, ResponseCallbacks,
    ResponseMessage,
};
use crate::error_::Result;
use crate::value::ValueReceive;

const SERVER_AGENT_KEY: &str = "server";
const PATCH_BOLT_KEY: &str = "patch_bolt";
const HINTS_KEY: &str = "hints";
const RECV_TIMEOUT_KEY: &str = "connection.recv_timeout_seconds";

#[derive(Debug)]
pub(crate) struct Bolt4x4<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> {
    translator: Arc<AtomicRefCell<T>>,
    bolt5x0: Bolt5x0<Arc<AtomicRefCell<T>>>,
    protocol_version: ServerAwareBoltVersion,
}

impl<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> Bolt4x4<T> {
    pub(in super::super) fn new(protocol_version: ServerAwareBoltVersion) -> Self {
        let translator: Arc<AtomicRefCell<T>> = Default::default();
        let bolt5x0 = Bolt5x0::new(Arc::clone(&translator), protocol_version);
        Bolt4x4 {
            translator,
            bolt5x0,
            protocol_version,
        }
    }

    pub(in super::super) fn write_utc_patch_entry(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        data: &BoltData<impl Read + Write>,
    ) -> Result<()> {
        serializer.write_string("patch_bolt")?;
        data.serialize_str_slice(serializer, &["utc"])?;
        debug_buf!(log_buf, "{}", {
            dbg_serializer.write_string("patch_bolt").unwrap();
            data.serialize_str_slice(dbg_serializer, &["utc"]).unwrap();
            dbg_serializer.flush()
        });
        Ok(())
    }

    pub(in super::super) fn hello_response_handle_utc_patch(
        hints: &HashMap<String, ValueReceive>,
        translator: &AtomicRefCell<T>,
    ) {
        if let Some(value) = hints.get(PATCH_BOLT_KEY) {
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
    }

    pub(in super::super) fn enqueue_hello_response(&self, data: &mut BoltData<impl Read + Write>) {
        let bolt_meta = Arc::clone(&data.meta);
        let bolt_server_agent = Arc::clone(&data.server_agent);
        let socket = Arc::clone(&data.socket);
        let translator = Arc::clone(&self.translator);

        data.responses.push_back(BoltResponse::new(
            ResponseMessage::Hello,
            ResponseCallbacks::new().with_on_success(move |mut meta| {
                Bolt5x0::<T>::hello_response_handle_agent(&mut meta, &bolt_server_agent);
                Self::hello_response_handle_utc_patch(&meta, &translator);
                Bolt5x0::<T>::hello_response_handle_connection_hints(
                    &meta,
                    socket.deref().as_ref(),
                );
                mem::swap(&mut *bolt_meta.borrow_mut(), &mut meta);
                Ok(())
            }),
        ));
    }
}

impl<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> Default for Bolt4x4<T> {
    fn default() -> Self {
        Self::new(ServerAwareBoltVersion::V4x4)
    }
}

impl<T: BoltStructTranslatorWithUtcPatch + Sync + Send + 'static> BoltProtocol for Bolt4x4<T> {
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
        self.bolt5x0
            .check_no_notification_filter(Some(notification_filter))?;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x01, 1)?;

        let extra_size = 2
            + <bool as Into<u64>>::into(routing_context.is_some())
            + u64::from_usize(auth.data.len());
        serializer.write_dict_header(extra_size)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.flush()
        });

        Bolt5x0::<T>::write_user_agent_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            user_agent,
        )?;

        Self::write_utc_patch_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, data)?;

        self.bolt5x0.write_routing_context_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            routing_context,
        )?;

        self.bolt5x0.write_auth_entries(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            auth,
        )?;
        data.auth = Some(Arc::clone(auth));

        data.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(data, log_buf);

        self.enqueue_hello_response(data);
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

    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x0.load_value(reader)
    }

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
