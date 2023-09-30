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

#![allow(clippy::option_map_unit_fn)]
// TODO: remove when prototyping phase is done
#![allow(dead_code)]

mod address;
pub mod driver;
mod error;
mod macros;
mod sync;
mod util;
pub mod value;

pub use address::Address;
pub use error::{Neo4jError, Result};
pub use value::ValueReceive;
pub use value::ValueSend;

pub mod bookmarks {
    pub use super::driver::session::bookmarks::*;
}
pub mod graph {
    pub use super::value::graph::*;
}
pub mod spatial {
    pub use super::value::spatial::*;
}
pub mod session {
    pub use super::driver::session::*;
}
pub mod retry {
    pub use super::driver::session::retry::*;
}
pub mod transaction {
    pub use super::driver::transaction::*;
}
/// Query summary structs (metadata) received via [`driver::RecordStream::consume()`].
pub mod summary {
    pub use super::driver::summary::*;
}

// TODO: decide if this concept should remain
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
enum Database<'a> {
    Named(&'a str),
    UnresolvedHome,
    ResolvedHome,
}

#[cfg(feature = "_internal_testkit_backend")]
pub mod testkit_backend;
