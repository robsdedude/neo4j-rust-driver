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

use crate::driver::notification::NotificationFilter;
use log::{debug, log_enabled, Level};
use usize_cast::FromUsize;

use super::super::bolt5x0::Bolt5x0;
use super::super::bolt5x1::Bolt5x1;
use super::super::bolt_common::ServerAwareBoltVersion;
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
    BoltResponse, BoltStructTranslator, OnServerErrorCb, ResponseCallbacks, ResponseMessage,
};
use crate::error_::Result;
use crate::value::ValueReceive;

#[derive(Debug)]
pub(crate) struct Bolt5x2<T: BoltStructTranslator> {
    translator: T,
    pub(in super::super) bolt5x1: Bolt5x1<T>,
}

impl<T: BoltStructTranslator> Bolt5x2<T> {
    pub(in super::super) fn new(protocol_version: ServerAwareBoltVersion) -> Self {
        Self {
            translator: T::default(),
            bolt5x1: Bolt5x1::new(protocol_version),
        }
    }

    pub(in super::super) fn notification_filter_entries_count(
        notification_filter: Option<&NotificationFilter>,
    ) -> u64 {
        match notification_filter {
            None => 0,
            Some(NotificationFilter {
                minimum_severity,
                disabled_categories,
            }) => [minimum_severity.is_some(), !disabled_categories.is_none()]
                .into_iter()
                .map(<bool as Into<u64>>::into)
                .sum(),
        }
    }

    pub(in super::super) fn write_notification_filter_entries(
        mut log_buf: Option<&mut String>,
        serializer: &mut PackStreamSerializerImpl<impl Write>,
        dbg_serializer: &mut PackStreamSerializerDebugImpl,
        notification_filter: Option<&NotificationFilter>,
    ) -> Result<()> {
        let Some(notification_filter) = notification_filter else {
            return Ok(());
        };
        let NotificationFilter {
            minimum_severity,
            disabled_categories,
        } = notification_filter;
        if let Some(minimum_severity) = minimum_severity {
            serializer.write_string("notifications_minimum_severity")?;
            serializer.write_string(minimum_severity.as_protocol_str())?;
            debug_buf!(log_buf, "{}", {
                dbg_serializer
                    .write_string("notifications_minimum_severity")
                    .unwrap();
                dbg_serializer
                    .write_string(minimum_severity.as_protocol_str())
                    .unwrap();
                dbg_serializer.flush()
            });
        }
        if let Some(disabled_categories) = disabled_categories {
            serializer.write_string("notifications_disabled_categories")?;
            serializer.write_list_header(u64::from_usize(disabled_categories.len()))?;
            for category in disabled_categories {
                serializer.write_string(category.as_protocol_str())?;
            }
            debug_buf!(log_buf, "{}", {
                dbg_serializer
                    .write_string("notifications_disabled_categories")
                    .unwrap();
                dbg_serializer
                    .write_list_header(u64::from_usize(disabled_categories.len()))
                    .unwrap();
                for category in disabled_categories {
                    dbg_serializer
                        .write_string(category.as_protocol_str())
                        .unwrap();
                }
                dbg_serializer.flush()
            });
        }
        Ok(())
    }
}

impl<T: BoltStructTranslator> Default for Bolt5x2<T> {
    fn default() -> Self {
        Self::new(ServerAwareBoltVersion::V5x2)
    }
}

impl<T: BoltStructTranslator> BoltProtocol for Bolt5x2<T> {
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

        let extra_size = 1
            + Self::notification_filter_entries_count(Some(notification_filter))
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

        self.bolt5x1.bolt5x0.write_routing_context_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            routing_context,
        )?;

        Self::write_notification_filter_entries(
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
        self.bolt5x1.reauth(data, parameters)
    }

    #[inline]
    fn supports_reauth(&self) -> bool {
        self.bolt5x1.supports_reauth()
    }

    #[inline]
    fn goodbye<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: GoodbyeParameters,
    ) -> Result<()> {
        self.bolt5x1.goodbye(data, parameters)
    }

    #[inline]
    fn reset<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: ResetParameters,
    ) -> Result<()> {
        self.bolt5x1.reset(data, parameters)
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

        self.bolt5x1.bolt5x0.write_parameter_dict(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            parameters,
        )?;

        let extra_size = Self::notification_filter_entries_count(notification_filter)
            + [
                bookmarks.is_some() && !bookmarks.unwrap().is_empty(),
                tx_timeout.is_some(),
                tx_metadata.is_some() && !tx_metadata.unwrap().is_empty(),
                mode.is_some() && mode.unwrap() != "w",
                db.is_some(),
                imp_user.is_some(),
            ]
            .into_iter()
            .map(<bool as Into<u64>>::into)
            .sum::<u64>();

        serializer.write_dict_header(extra_size)?;
        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.flush()
        });

        Bolt5x0::<T>::write_bookmarks_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            bookmarks,
        )?;

        Bolt5x0::<T>::write_tx_timeout_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            tx_timeout,
        )?;

        self.bolt5x1.bolt5x0.write_tx_metadata_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            data,
            tx_metadata,
        )?;

        Bolt5x0::<T>::write_mode_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            mode,
        )?;

        Bolt5x0::<T>::write_db_entry(log_buf.as_mut(), &mut serializer, &mut dbg_serializer, db)?;

        Bolt5x0::<T>::write_imp_user_entry(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            imp_user,
        )?;

        Self::write_notification_filter_entries(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            notification_filter,
        )?;

        callbacks = Bolt5x0::<T>::install_qid_hook(data, callbacks);

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::new(ResponseMessage::Run, callbacks));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    #[inline]
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: DiscardParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x1.discard(data, parameters, callbacks)
    }

    #[inline]
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: PullParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x1.pull(data, parameters, callbacks)
    }

    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: BeginParameters<K>,
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
        debug_buf_start!(log_buf);
        debug_buf!(log_buf, "C: BEGIN");
        let mut dbg_serializer = PackStreamSerializerDebugImpl::new();
        let mut message_buff = Vec::new();
        let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
        serializer.write_struct_header(0x11, 1)?;

        let extra_size = Self::notification_filter_entries_count(Some(notification_filter))
            + [
                bookmarks.is_some() && !bookmarks.unwrap().is_empty(),
                tx_timeout.is_some(),
                tx_metadata.map(|m| !m.is_empty()).unwrap_or_default(),
                mode.is_some() && mode.unwrap() != "w",
                db.is_some(),
                imp_user.is_some(),
            ]
            .into_iter()
            .map(<bool as Into<u64>>::into)
            .sum::<u64>();

        debug_buf!(log_buf, " {}", {
            dbg_serializer.write_dict_header(extra_size).unwrap();
            dbg_serializer.flush()
        });
        serializer.write_dict_header(extra_size)?;

        if let Some(bookmarks) = bookmarks {
            if !bookmarks.is_empty() {
                debug_buf!(log_buf, "{}", {
                    dbg_serializer.write_string("bookmarks").unwrap();
                    data.serialize_str_iter(&mut dbg_serializer, bookmarks.raw())
                        .unwrap();
                    dbg_serializer.flush()
                });
                serializer.write_string("bookmarks").unwrap();
                data.serialize_str_iter(&mut serializer, bookmarks.raw())?;
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
                    data.serialize_dict(&mut dbg_serializer, &self.translator, tx_metadata)
                        .unwrap();
                    dbg_serializer.flush()
                });
                serializer.write_string("tx_metadata")?;
                data.serialize_dict(&mut serializer, &self.translator, tx_metadata)?;
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

        Self::write_notification_filter_entries(
            log_buf.as_mut(),
            &mut serializer,
            &mut dbg_serializer,
            Some(notification_filter),
        )?;

        data.message_buff.push_back(vec![message_buff]);
        data.responses
            .push_back(BoltResponse::from_message(ResponseMessage::Begin));
        debug_buf_end!(data, log_buf);
        Ok(())
    }

    #[inline]
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: CommitParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x1.commit(data, parameters, callbacks)
    }

    #[inline]
    fn rollback<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RollbackParameters,
    ) -> Result<()> {
        self.bolt5x1.rollback(data, parameters)
    }

    #[inline]
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RouteParameters,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.bolt5x1.route(data, parameters, callbacks)
    }

    #[inline]
    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive> {
        self.bolt5x1.load_value(reader)
    }

    #[inline]
    fn handle_response<RW: Read + Write>(
        &mut self,
        bolt_data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb<RW>,
    ) -> Result<()> {
        self.bolt5x1
            .handle_response(bolt_data, message, on_server_error)
    }
}
