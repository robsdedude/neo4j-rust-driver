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

    // pub(crate) fn onSuccess<F: FnMut(HashMap<String, Value>) -> Result<()> + 'static>(
    //     &mut self,
    //     cb: F,
    // ) {
    //     self.on_success.push(Box::new(cb));
    // }
    //
    // pub(crate) fn callOnSuccess(&mut self, meta: Value) -> Result<()> {
    //     match meta {
    //         Value::Map(meta) => {
    //             let metas = vec![meta; self.on_success.len()].into_iter();
    //             self.on_success
    //                 .iter_mut()
    //                 .zip(metas)
    //                 .map(|(cb, meta)| cb(meta))
    //                 .collect::<Result<_>>()?;
    //             Ok(())
    //         }
    //         _ => Err(Neo4jError::ProtocolError {
    //             message: "onSuccess meta was not a Dictionary".into(),
    //         }),
    //     }
    // }

    // pub(crate) fn onFailure<F: FnMut(&HashMap<String, Value>) -> Result<()>>(&mut self, cb: F) {
    //     self.on_failure.push(Box::new(cb));
    // }
    //
    // pub(crate) fn callOnFailure<F: FnMut(HashMap<String, Value>) -> Result<()>>(
    //     &self,
    //     meta: Value,
    // ) -> Result<()> {
    //     match meta {
    //         Value::Map(meta) => self
    //             .on_failure
    //             .iter()
    //             .zip(vec![meta; self.on_failure.len()].into_iter())
    //             .each(|(cb, meta)| cb(meta))
    //             .collect(),
    //         _ => {
    //             return Err(Neo4jError::ProtocolError {
    //                 message: "onFailure meta was not a Dictionary".into(),
    //             })
    //         }
    //     }
    //     Ok(())
    // }
}

pub(crate) struct ResponseCallbacks {
    on_success_cb: Option<Box<dyn FnMut(HashMap<String, Value>) -> Result<()>>>,
    on_record_cb: Option<Box<dyn FnMut(Vec<Value>) -> Result<()>>>,
    on_failure_cb: Option<Box<dyn FnMut(HashMap<String, Value>) -> Result<()>>>,
}

impl ResponseCallbacks {
    pub(crate) fn new() -> Self {
        Self {
            on_success_cb: None,
            on_record_cb: None,
            on_failure_cb: None,
        }
    }

    pub(crate) fn with_on_success<F: FnMut(HashMap<String, Value>) -> Result<()> + 'static>(
        mut self,
        cb: F,
    ) -> Self {
        self.on_success_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn on_success(&mut self, meta: Value) -> Result<()> {
        match meta {
            Value::Map(meta) => match self.on_success_cb.as_mut() {
                None => Ok(()),
                Some(cb) => cb(meta),
            },
            _ => Err(Neo4jError::ProtocolError {
                message: "onSuccess meta was not a Dictionary".into(),
            }),
        }
    }

    pub(crate) fn with_on_record<F: FnMut(Vec<Value>) -> Result<()> + 'static>(
        mut self,
        cb: F,
    ) -> Self {
        self.on_record_cb = Some(Box::new(cb));
        self
    }

    pub(crate) fn on_record(&mut self, meta: Value) -> Result<()> {
        match meta {
            Value::List(values) => match self.on_record_cb.as_mut() {
                None => Ok(()),
                Some(cb) => cb(values),
            },
            _ => Err(Neo4jError::ProtocolError {
                message: "onRecord meta was not a List".into(),
            }),
        }
    }
}

impl Debug for ResponseCallbacks {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "ResponseCallbacks {{\n  on_success: {:?}\n  #on_record: {:?}\n  #on_failure: {:?}\n}}",
            self.on_success_cb.as_ref().map(|_| "..."),
            self.on_record_cb.as_ref().map(|_| "..."),
            self.on_failure_cb.as_ref().map(|_| "...")
        )
    }
}
