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

use super::super::bolt5x4::Bolt5x4;
use super::super::bolt_common::ServerAwareBoltVersion;
use super::super::message::BoltMessage;
use super::super::message_parameters::{
    BeginParameters, CommitParameters, DiscardParameters, GoodbyeParameters, HelloParameters,
    PullParameters, ReauthParameters, ResetParameters, RollbackParameters, RouteParameters,
    RunParameters, TelemetryParameters,
};
use super::super::{
    BoltData, BoltProtocol, BoltStructTranslator, OnServerErrorCb, ResponseCallbacks,
};
use crate::error_::Result;
use crate::value::ValueReceive;

#[derive(Debug)]
pub(crate) struct Bolt5x6<T: BoltStructTranslator> {
    pub(in super::super) bolt5x4: Bolt5x4<T>,
}

impl<T: BoltStructTranslator> Bolt5x6<T> {
    pub(in super::super) fn new(protocol_version: ServerAwareBoltVersion) -> Self {
        Self {
            bolt5x4: Bolt5x4::new(protocol_version),
        }
    }

    pub(in super::super) fn make_enrich_diag_record_callback(
        callbacks: ResponseCallbacks,
    ) -> ResponseCallbacks {
        callbacks.with_on_success_pre_hook(|meta| {
            if meta
                .get("has_more")
                .and_then(ValueReceive::as_bool)
                .unwrap_or_default()
            {
                return Ok(());
            }
            let Some(statuses) = meta.get_mut("statuses").and_then(ValueReceive::as_list_mut)
            else {
                return Ok(());
            };
            for status in statuses.iter_mut() {
                let Some(status) = status.as_map_mut() else {
                    continue;
                };
                let Some(diag_record) = status
                    .entry(String::from("diagnostic_record"))
                    .or_insert_with(|| ValueReceive::Map(HashMap::with_capacity(3)))
                    .as_map_mut()
                else {
                    continue;
                };
                for (key, value) in &[
                    ("OPERATION", ""),
                    ("OPERATION_CODE", "0"),
                    ("CURRENT_SCHEMA", "/"),
                ] {
                    diag_record
                        .entry(String::from(*key))
                        .or_insert_with(|| ValueReceive::String(String::from(*value)));
                }
            }
            Ok(())
        })
    }
}

impl<T: BoltStructTranslator> Default for Bolt5x6<T> {
    fn default() -> Self {
        Self::new(ServerAwareBoltVersion::V5x6)
    }
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x6<T> {
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: HelloParameters,
    ) -> Result<()> {
        self.bolt5x4.hello(data, parameters)
    }

    #[inline]
    fn reauth<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ReauthParameters,
    ) -> Result<()> {
        self.bolt5x4.reauth(data, parameters)
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        self.bolt5x4.supports_reauth()
    }

    #[inline]
    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: GoodbyeParameters,
    ) -> Result<()> {
        self.bolt5x4.goodbye(data, parameters)
    }

    #[inline]
    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ResetParameters,
    ) -> Result<()> {
        self.bolt5x4.reset(data, parameters)
    }

    #[inline]
    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let callbacks = Self::make_enrich_diag_record_callback(callbacks);
        self.bolt5x4.run(data, parameters, callbacks)
    }

    #[inline]
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x4.discard(data, parameters, callbacks)
    }

    #[inline]
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        let callbacks = Self::make_enrich_diag_record_callback(callbacks);
        self.bolt5x4.pull(data, parameters, callbacks)
    }

    #[inline]
    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x4.begin(data, parameters, callbacks)
    }

    #[inline]
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x4.commit(data, parameters, callbacks)
    }

    #[inline]
    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RollbackParameters,
    ) -> Result<()> {
        self.bolt5x4.rollback(data, parameters)
    }

    #[inline]
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x4.route(data, parameters, callbacks)
    }

    fn telemetry<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: TelemetryParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x4.telemetry(data, parameters, callbacks)
    }

    #[inline]
    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x4.load_value(reader)
    }

    #[inline]
    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        self.bolt5x4
            .handle_response(bolt_data, message, on_server_error)
    }
}
