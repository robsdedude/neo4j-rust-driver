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
use std::io::{Read, Write};

use log::{debug, log_enabled, Level};

use super::super::bolt5x0::Bolt5x0;
use super::super::bolt5x2::Bolt5x2;
use super::super::bolt_common::{
    ServerAwareBoltVersion, BOLT_AGENT_LANGUAGE, BOLT_AGENT_LANGUAGE_DETAILS, BOLT_AGENT_PLATFORM,
    BOLT_AGENT_PRODUCT,
};
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
    BoltStructTranslator, OnServerErrorCb, ResponseCallbacks,
};
use crate::error_::Result;
use crate::value::ValueReceive;

const SERVER_AGENT_KEY: &str = "server";
const HINTS_KEY: &str = "hints";
const RECV_TIMEOUT_KEY: &str = "connection.recv_timeout_seconds";

#[derive(Debug)]
pub(crate) struct Bolt5x3<T: BoltStructTranslator> {
    translator: T,
    pub(in super::super) bolt5x2: Bolt5x2<T>,
    protocol_version: ServerAwareBoltVersion,
}

impl<T: BoltStructTranslator> Bolt5x3<T> {
    pub(in super::super) fn new(protocol_version: ServerAwareBoltVersion) -> Self {
        Self {
            translator: T::default(),
            bolt5x2: Bolt5x2::new(protocol_version),
            protocol_version,
        }
    }

    pub(in super::super) fn write_bolt_agent_entry(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
    ) -> Result<()> {
        serializer.write_string("bolt_agent")?;
        serializer.write_dict_header(4)?;
        serializer.write_string("product")?;
        serializer.write_string(BOLT_AGENT_PRODUCT)?;
        serializer.write_string("platform")?;
        serializer.write_string(BOLT_AGENT_PLATFORM)?;
        serializer.write_string("language")?;
        serializer.write_string(BOLT_AGENT_LANGUAGE)?;
        serializer.write_string("language_details")?;
        serializer.write_string(BOLT_AGENT_LANGUAGE_DETAILS)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_string("bolt_agent").unwrap();
            dbg_serializer.write_dict_header(4).unwrap();
            dbg_serializer.write_string("product").unwrap();
            dbg_serializer.write_string(BOLT_AGENT_PRODUCT).unwrap();
            dbg_serializer.write_string("platform").unwrap();
            dbg_serializer.write_string(BOLT_AGENT_PLATFORM).unwrap();
            dbg_serializer.write_string("language").unwrap();
            dbg_serializer.write_string(BOLT_AGENT_LANGUAGE).unwrap();
            dbg_serializer.write_string("language_details").unwrap();
            dbg_serializer
                .write_string(BOLT_AGENT_LANGUAGE_DETAILS)
                .unwrap();
            dbg_serializer.flush()
        });
        Ok(())
    }
}

impl<T: BoltStructTranslator> Default for Bolt5x3<T> {
    fn default() -> Self {
        Self::new(ServerAwareBoltVersion::V5x3)
    }
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x3<T> {
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

        Self::write_bolt_agent_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer)?;

        self.bolt5x2.bolt5x1.bolt5x0.write_routing_context_entry(
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

        Bolt5x0::<T>::enqueue_hello_response(data);
        Ok(())
    }

    #[inline]
    fn reauth<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ReauthParameters,
    ) -> Result<()> {
        self.bolt5x2.reauth(data, parameters)
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        self.bolt5x2.supports_reauth()
    }

    #[inline]
    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: GoodbyeParameters,
    ) -> Result<()> {
        self.bolt5x2.goodbye(data, parameters)
    }

    #[inline]
    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ResetParameters,
    ) -> Result<()> {
        self.bolt5x2.reset(data, parameters)
    }

    #[inline]
    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x2.run(data, parameters, callbacks)
    }

    #[inline]
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x2.discard(data, parameters, callbacks)
    }

    #[inline]
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x2.pull(data, parameters, callbacks)
    }

    #[inline]
    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
    ) -> Result<()> {
        self.bolt5x2.begin(data, parameters)
    }

    #[inline]
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x2.commit(data, parameters, callbacks)
    }

    #[inline]
    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RollbackParameters,
    ) -> Result<()> {
        self.bolt5x2.rollback(data, parameters)
    }

    #[inline]
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x2.route(data, parameters, callbacks)
    }

    #[inline]
    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x2.load_value(reader)
    }

    #[inline]
    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        self.bolt5x2
            .handle_response(bolt_data, message, on_server_error)
    }
}
