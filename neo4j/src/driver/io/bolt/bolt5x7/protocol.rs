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

use crate::error::ServerError;
use log::warn;

use super::super::bolt5x6::Bolt5x6;
use super::super::bolt_common::ServerAwareBoltVersion;
use super::super::message::BoltMessage;
use super::super::message_parameters::{
    BeginParameters, CommitParameters, DiscardParameters, GoodbyeParameters, HelloParameters,
    PullParameters, ReauthParameters, ResetParameters, RollbackParameters, RouteParameters,
    RunParameters, TelemetryParameters,
};
use super::super::response::BoltMeta;
use super::super::{
    assert_response_field_count, bolt_debug, BoltData, BoltProtocol, BoltStructTranslator,
    OnServerErrorCb, ResponseCallbacks,
};
use crate::error_::Result;
use crate::value::ValueReceive;
use crate::Neo4jError;

#[derive(Debug)]
pub(crate) struct Bolt5x7<T: BoltStructTranslator> {
    pub(in super::super) bolt5x6: Bolt5x6<T>,
}

impl<T: BoltStructTranslator> Bolt5x7<T> {
    pub(in super::super) fn new(protocol_version: ServerAwareBoltVersion) -> Self {
        Self {
            bolt5x6: Bolt5x6::new(protocol_version),
        }
    }

    pub(in super::super) fn try_parse_error(meta: ValueReceive) -> Result<ServerError> {
        let meta = meta
            .try_into_map()
            .map_err(|_| Neo4jError::protocol_error("FAILURE meta was not a Dictionary"))?;
        Ok(ServerError::from_meta_gql(meta))
    }

    pub(in super::super) fn enrich_failure_diag_record(mut meta: &mut BoltMeta) {
        loop {
            if let Some(diag_record) = meta
                .entry(String::from("diagnostic_record"))
                .or_insert_with(|| ValueReceive::Map(HashMap::with_capacity(3)))
                .as_map_mut()
            {
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
            match meta.get_mut("cause").and_then(ValueReceive::as_map_mut) {
                None => break,
                Some(cause) => meta = cause,
            };
        }
    }
}

impl<T: BoltStructTranslator> Default for Bolt5x7<T> {
    fn default() -> Self {
        Self::new(ServerAwareBoltVersion::V5x7)
    }
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x7<T> {
    #[inline]
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: HelloParameters,
    ) -> Result<()> {
        self.bolt5x6.hello(data, parameters)
    }

    #[inline]
    fn reauth<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ReauthParameters,
    ) -> Result<()> {
        self.bolt5x6.reauth(data, parameters)
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        self.bolt5x6.supports_reauth()
    }

    #[inline]
    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: GoodbyeParameters,
    ) -> Result<()> {
        self.bolt5x6.goodbye(data, parameters)
    }

    #[inline]
    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ResetParameters,
    ) -> Result<()> {
        self.bolt5x6.reset(data, parameters)
    }

    #[inline]
    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.run(data, parameters, callbacks)
    }

    #[inline]
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.discard(data, parameters, callbacks)
    }

    #[inline]
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.pull(data, parameters, callbacks)
    }

    #[inline]
    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.begin(data, parameters, callbacks)
    }

    #[inline]
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.commit(data, parameters, callbacks)
    }

    #[inline]
    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RollbackParameters,
    ) -> Result<()> {
        self.bolt5x6.rollback(data, parameters)
    }

    #[inline]
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.route(data, parameters, callbacks)
    }

    #[inline]
    fn telemetry<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: TelemetryParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x6.telemetry(data, parameters, callbacks)
    }

    #[inline]
    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x6.load_value(reader)
    }

    #[inline]
    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        match message {
            BoltMessage {
                tag: 0x7F,
                mut fields,
            } => {
                // FAILURE
                let mut response = bolt_data
                    .responses
                    .pop_front()
                    .expect("called Bolt::read_one with empty response queue");

                assert_response_field_count("FAILURE", &fields, 1)?;
                let mut meta = fields.pop().unwrap();
                bolt_debug!(bolt_data, "S: FAILURE {}", meta.dbg_print());
                meta.as_map_mut().map(Self::enrich_failure_diag_record);
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
            message => self
                .bolt5x6
                .handle_response(bolt_data, message, on_server_error),
        }
    }
}
