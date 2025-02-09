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

use crate::driver::io::bolt::bolt5x4::Bolt5x4;
use crate::driver::io::bolt::{BoltResponse, ResponseMessage};
use log::{debug, log_enabled, warn};

use super::super::bolt_common::ServerAwareBoltVersion;
use super::super::message::BoltMessage;
use super::super::message_parameters::{
    BeginParameters, CommitParameters, DiscardParameters, GoodbyeParameters, HelloParameters,
    PullParameters, ReauthParameters, ResetParameters, RollbackParameters, RouteParameters,
    RunParameters, TelemetryParameters,
};
use super::super::packstream::{
    PackStreamSerializer, PackStreamSerializerDebugImpl, PackStreamSerializerImpl,
};
use super::super::response::BoltMeta;
use super::super::{bolt5x0::Bolt5x0, bolt5x2::Bolt5x2, bolt5x3::Bolt5x3, bolt5x7::Bolt5x7};
use super::super::{
    bolt_debug_extra, debug_buf, debug_buf_end, debug_buf_start, BoltData, BoltProtocol,
    BoltStructTranslator, OnServerErrorCb, ResponseCallbacks,
};
use crate::error_::Result;
use crate::value::ValueReceive;

const HINTS_KEY: &str = "hints";
const SSR_ENABLED_KEY: &str = "ssr.enabled";

#[derive(Debug)]
pub(crate) struct Bolt5x8<T: BoltStructTranslator> {
    pub(in super::super) bolt5x7: Bolt5x7<T>,
}

impl<T: BoltStructTranslator> Bolt5x8<T> {
    pub(in super::super) fn new(protocol_version: ServerAwareBoltVersion) -> Self {
        Self {
            bolt5x7: Bolt5x7::new(protocol_version),
        }
    }

    pub(in super::super) fn enqueue_hello_response(data: &mut BoltData<impl Read + Write>) {
        let bolt_meta = Arc::clone(&data.meta);
        let telemetry_enabled = Arc::clone(&data.telemetry_enabled);
        let ssr_enabled = Arc::clone(&data.ssr_enabled);
        let bolt_server_agent = Arc::clone(&data.server_agent);
        let socket = Arc::clone(&data.socket);

        data.responses.push_back(BoltResponse::new(
            ResponseMessage::Hello,
            ResponseCallbacks::new().with_on_success(move |mut meta| {
                Bolt5x0::<T>::hello_response_handle_agent(&mut meta, &bolt_server_agent);
                Self::hello_response_handle_connection_hints(
                    &meta,
                    socket.deref().as_ref(),
                    &mut telemetry_enabled.borrow_mut(),
                    &mut ssr_enabled.borrow_mut(),
                );
                mem::swap(&mut *bolt_meta.borrow_mut(), &mut meta);
                Ok(())
            }),
        ));
    }

    pub(in super::super) fn hello_response_handle_connection_hints(
        meta: &BoltMeta,
        socket: Option<&TcpStream>,
        telemetry_enabled: &mut bool,
        ssr_enabled: &mut bool,
    ) {
        let empty_hints = HashMap::new();
        let hints = match meta.get(HINTS_KEY) {
            Some(ValueReceive::Map(hints)) => hints,
            Some(value) => {
                warn!("Server sent unexpected {HINTS_KEY} type {:?}", value);
                &empty_hints
            }
            None => &empty_hints,
        };
        Bolt5x0::<T>::hello_response_handle_timeout_hint(hints, socket);
        Bolt5x4::<T>::hello_response_telemetry_hint(hints, telemetry_enabled);
        Self::hello_response_handle_ssr_enabled_hint(hints, ssr_enabled);
    }

    pub(in super::super) fn hello_response_handle_ssr_enabled_hint(
        hints: &HashMap<String, ValueReceive>,
        ssr_enabled: &mut bool,
    ) {
        match hints.get(SSR_ENABLED_KEY) {
            None => {
                *ssr_enabled = false;
            }
            Some(ValueReceive::Boolean(value)) => *ssr_enabled = *value,
            Some(value) => {
                *ssr_enabled = false;
                warn!("Server sent unexpected {SSR_ENABLED_KEY} type {:?}", value);
            }
        }
    }
}

impl<T: BoltStructTranslator> Default for Bolt5x8<T> {
    fn default() -> Self {
        Self::new(ServerAwareBoltVersion::V5x8)
    }
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x8<T> {
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: HelloParameters,
    ) -> Result<()> {
        let HelloParameters {
            user_agent,
            auth: _,
            routing_context,
            notification_filter,
        } = parameters;
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: HELLO");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x01, 1)?;

        let extra_size = 2
            + Bolt5x2::<T>::notification_filter_entries_count(Some(notification_filter))
            + <bool as Into<u64>>::into(routing_context.is_some());

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

        Bolt5x3::<T>::write_bolt_agent_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
        )?;

        self.bolt5x7
            .bolt5x6
            .bolt5x4
            .bolt5x3
            .bolt5x2
            .bolt5x1
            .bolt5x0
            .write_routing_context_entry(
                log_buf.as_mut(),
                &mut serializer,
                &mut dbg_serializer,
                data,
                routing_context,
            )?;

        Bolt5x2::<T>::write_notification_filter_entries(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            Some(notification_filter),
        )?;

        data.message_buff.push_back(vec![message_buff]);
        debug_buf_end!(data, log_buf);

        Self::enqueue_hello_response(data);
        Ok(())
    }

    #[inline]
    fn reauth<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ReauthParameters,
    ) -> Result<()> {
        self.bolt5x7.reauth(data, parameters)
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        self.bolt5x7.supports_reauth()
    }

    #[inline]
    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: GoodbyeParameters,
    ) -> Result<()> {
        self.bolt5x7.goodbye(data, parameters)
    }

    #[inline]
    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ResetParameters,
    ) -> Result<()> {
        self.bolt5x7.reset(data, parameters)
    }

    #[inline]
    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.run(data, parameters, callbacks)
    }

    #[inline]
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.discard(data, parameters, callbacks)
    }

    #[inline]
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.pull(data, parameters, callbacks)
    }

    #[inline]
    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.begin(data, parameters, callbacks)
    }

    #[inline]
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.commit(data, parameters, callbacks)
    }

    #[inline]
    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RollbackParameters,
    ) -> Result<()> {
        self.bolt5x7.rollback(data, parameters)
    }

    #[inline]
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.route(data, parameters, callbacks)
    }

    #[inline]
    fn telemetry<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: TelemetryParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x7.telemetry(data, parameters, callbacks)
    }

    #[inline]
    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x7.load_value(reader)
    }

    #[inline]
    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        self.bolt5x7
            .handle_response(bolt_data, message, on_server_error)
    }
}
