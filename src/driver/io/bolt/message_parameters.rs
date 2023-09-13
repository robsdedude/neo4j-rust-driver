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

use crate::ValueSend;

#[derive(Debug)]
pub(crate) struct RunParameters<'a, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug> {
    pub(super) query: &'a str,
    pub(super) parameters: Option<&'a HashMap<KP, ValueSend>>,
    pub(super) bookmarks: Option<&'a [String]>,
    pub(super) tx_timeout: Option<i64>,
    pub(super) tx_metadata: Option<&'a HashMap<KM, ValueSend>>,
    pub(super) mode: Option<&'a str>,
    pub(super) db: Option<&'a str>,
    pub(super) imp_user: Option<&'a str>,
}

impl<'a, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug> RunParameters<'a, KP, KM> {
    #[allow(clippy::too_many_arguments)] // builder pattern for internal API seems overkill
    pub(crate) fn new_auto_commit_run(
        query: &'a str,
        parameters: Option<&'a HashMap<KP, ValueSend>>,
        bookmarks: Option<&'a [String]>,
        tx_timeout: Option<i64>,
        tx_metadata: Option<&'a HashMap<KM, ValueSend>>,
        mode: Option<&'a str>,
        db: Option<&'a str>,
        imp_user: Option<&'a str>,
    ) -> Self {
        Self {
            query,
            parameters,
            bookmarks,
            tx_timeout,
            tx_metadata,
            mode,
            db,
            imp_user,
        }
    }
}

impl<'a, KP: Borrow<str> + Debug> RunParameters<'a, KP, String> {
    pub(crate) fn new_transaction_run(
        query: &'a str,
        parameters: Option<&'a HashMap<KP, ValueSend>>,
    ) -> Self {
        Self {
            query,
            parameters,
            bookmarks: None,
            tx_timeout: None,
            tx_metadata: None,
            mode: None,
            db: None,
            imp_user: None,
        }
    }
}
