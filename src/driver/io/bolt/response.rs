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

use crate::{Neo4jError, Result, Value};
use core::fmt::{Debug, Formatter};
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) enum ResponseMessage {
    Hello,
    Reset,
    Run,
    Discard,
    Pull,
    Begin,
    Commit,
    Rollback,
    Route,
}

#[derive(Debug)]
pub(crate) struct BoltResponse {
    pub(crate) message: ResponseMessage,
    pub(crate) callbacks: ResponseCallbacks,
}

impl BoltResponse {
    pub(crate) fn new(message: ResponseMessage, callbacks: ResponseCallbacks) -> Self {
        Self { message, callbacks }
    }

    pub(crate) fn from_message(message: ResponseMessage) -> Self {
        Self::new(message, ResponseCallbacks::new())
    }

    pub(crate) fn is_summary(meta: &Value) -> bool {
        match &meta {
            Value::Map(m) => match m.get("has_more") {
                Some(Value::Boolean(b)) => !*b,
                _ => true,
            },
            _ => true,
        }
    }
}

type OptBox<T> = Option<Box<T>>;
pub(crate) type BoltMeta = HashMap<String, Value>;
pub(crate) type BoltRecordFields = Vec<Value>;

pub(crate) struct ResponseCallbacks {
    on_success_cb: OptBox<dyn FnMut(BoltMeta) -> Result<()>>,
    on_failure_cb: OptBox<dyn FnMut(BoltMeta) -> Result<()>>,
    on_ignored_cb: OptBox<dyn FnMut() -> Result<()>>,
    on_record_cb: OptBox<dyn FnMut(BoltRecordFields) -> Result<()>>,
    on_summary_cb: OptBox<dyn FnMut()>,
}

impl ResponseCallbacks {
    pub(crate) fn new() -> Self {
        Self {
            on_success_cb: None,
            on_failure_cb: None,
            on_ignored_cb: None,
            on_record_cb: None,
            on_summary_cb: None,
        }
    }

    pub(crate) fn with_on_success<F: FnMut(BoltMeta) -> Result<()> + 'static>(
        mut self,
        cb: F,
    ) -> Self {
        self.on_success_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn with_on_failure<F: FnMut(BoltMeta) -> Result<()> + 'static>(
        mut self,
        cb: F,
    ) -> Self {
        self.on_failure_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn with_on_ignored<F: FnMut() -> Result<()> + 'static>(mut self, cb: F) -> Self {
        self.on_ignored_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn with_on_record<F: FnMut(BoltRecordFields) -> Result<()> + 'static>(
        mut self,
        cb: F,
    ) -> Self {
        self.on_record_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn with_on_summary<F: FnMut() + 'static>(mut self, cb: F) -> Self {
        self.on_summary_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn on_success(&mut self, meta: Value) -> Result<()> {
        let is_summary = BoltResponse::is_summary(&meta);
        let res = match meta {
            Value::Map(meta) => match self.on_success_cb.as_mut() {
                None => Ok(()),
                Some(cb) => cb(meta),
            },
            _ => Err(Neo4jError::ProtocolError {
                message: "onSuccess meta was not a Dictionary".into(),
            }),
        };
        if is_summary {
            self.on_summary();
        }
        res
    }

    pub(crate) fn on_failure(&mut self, meta: Value) -> Result<()> {
        let res = match meta {
            Value::Map(meta) => match self.on_failure_cb.as_mut() {
                None => Ok(()),
                Some(cb) => cb(meta),
            },
            _ => Err(Neo4jError::ProtocolError {
                message: "onFailure meta was not a Dictionary".into(),
            }),
        };
        self.on_summary();
        res
    }

    pub(crate) fn on_ignored(&mut self) -> Result<()> {
        let res = self.on_ignored_cb.as_mut().map(|cb| cb()).unwrap_or(Ok(()));
        self.on_summary();
        res
    }

    pub(crate) fn on_record(&mut self, data: Value) -> Result<()> {
        match data {
            Value::List(values) => match self.on_record_cb.as_mut() {
                None => Ok(()),
                Some(cb) => cb(values),
            },
            _ => Err(Neo4jError::ProtocolError {
                message: "onRecord data was not a List".into(),
            }),
        }
    }

    fn on_summary(&mut self) {
        if let Some(cb) = self.on_summary_cb.as_mut() {
            cb()
        }
    }
}

impl Debug for ResponseCallbacks {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "ResponseCallbacks {{\n  on_success: {:?}\n  #on_failure: {:?}\n  #on_ignored: {:?}\n  \
             #on_record: {:?}\n  #on_summary: {:?}\n}}",
            self.on_success_cb.as_ref().map(|_| "..."),
            self.on_failure_cb.as_ref().map(|_| "..."),
            self.on_ignored_cb.as_ref().map(|_| "..."),
            self.on_record_cb.as_ref().map(|_| "..."),
            self.on_summary_cb.as_ref().map(|_| "..."),
        )
    }
}
