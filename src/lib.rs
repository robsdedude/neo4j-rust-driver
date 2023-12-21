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

//! # Neo4j Bolt Driver
//!
//! This crate provides a driver for the Neo4j graph database.
//! It's designed to mirror many concepts of the official drivers while leveraging Rust's expressive
//! type system and lifetime management to provide a safer API that prevents many common pitfalls
//! already at compile time.
//!
//! ## Basic Example
//! ```
//! use std::sync::Arc;
//! # use std::env;
//!
//! use neo4j::address::Address;
//! use neo4j::driver::auth::AuthToken;
//! use neo4j::driver::{ConnectionConfig, Driver, DriverConfig, RoutingControl};
//! use neo4j::retry::ExponentialBackoff;
//! use neo4j::{value_map, ValueReceive};
//!
//! let host = "localhost";
//! # let host = doc_test_utils::get_host();
//! let port = 7687;
//! # let port = doc_test_utils::get_port();
//! let user = "neo4j";
//! # let user = doc_test_utils::get_user();
//! let password = "pass";
//! # let password = doc_test_utils::get_password();
//! let database = "neo4j";
//!
//! let database = Arc::new(String::from(database));
//! let address = Address::from((host, port));
//! let auth_token = AuthToken::new_basic_auth(user, password);
//! let driver = Driver::new(
//!     // tell the driver where to connect to
//!     ConnectionConfig::new(address),
//!     // configure how the driver works locally (e.g., authentication)
//!     DriverConfig::new().with_auth(Arc::new(auth_token)),
//! );
//!
//! // Driver::execute_query() is tne easiest way to run a query.
//! // It will be sufficient for most use cases and allows the driver to apply some optimizations.
//! // So it's recommended to use it whenever possible.
//! // For more control, see sessions and transactions.
//! let result = driver
//!     // Run a CYPHER query against the DBMS.
//!     .execute_query("RETURN $x AS x")
//!     // Always specify the database when you can (also applies to using sessions).
//!     // This will let the driver work more efficiently.
//!     .with_database(database)
//!     // Tell the driver to send the query to a read server.
//!     // In a clustered environment, this will allow make sure that read queries don't overload
//!     // the write single server.
//!     .with_routing_control(RoutingControl::Read)
//!     // Use query parameters (instead of string interpolation) to avoid injection attacks and
//!     // improve performance.
//!     .with_parameters(value_map!(
//!         {"x": 123}
//!     ))
//!     // For more resilience, use retry policies.
//!     .run_with_retry(ExponentialBackoff::default());
//! println!("{:?}", result);
//!
//! let result = result.unwrap();
//! assert_eq!(result.records.len(), 1);
//! for record in result.records {
//!     assert_eq!(
//!         record.entries,
//!         vec![(Arc::new(String::from("x")), ValueReceive::Integer(123))]
//!     );
//! }
//! ```
//!
//! ## Concepts
//!
//! ### The Driver
//! The fundamental type of this crate is the [`driver::Driver`].
//! Through it, all database interactions are performed.
//! See [`driver::Driver::new()`].
//! The driver manages a connection pool. So there is no need to pool driver objects.
//! Usually, each application will use one global driver.
//!
//! ### Sessions
//! Sessions are spawned from the driver.
//! See [`driver::Driver::session()`].
//! Session creation is cheap, it's recommended to create a new session for each piece of work
//! (unless when using [`Driver::execute_query()`]).
//! Sessions will borrows connections from the driver's pool as needed.
//!
//! ### Main Mechanisms for Query Execution
//! There are three main ways to execute queries:
//! - [`Driver::execute_query()`] is the easiest way to run a query.
//!   Prefer it whenever possible.
//! - [`Session::transaction()`] gives you full control over the transaction.
//! - [`Session::auto_commit()`] is a special method for running queries that manage their own
//!   transactions, such as `CALL {...} IN TRANSACTION`.

mod address_;
pub mod driver;
mod error;
mod macros;
mod sync;
mod time;
mod util;
pub mod value;

// imports for docs
#[allow(unused)]
use driver::Driver;
#[allow(unused)]
use session::Session;

pub use error::{Neo4jError, Result};
pub use value::ValueReceive;
pub use value::ValueSend;

pub mod address {
    pub use super::address_::resolution::*;
    pub use super::address_::*;
}
pub mod bookmarks {
    pub use super::driver::session::bookmarks::*;
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
/// Query summary structs (metadata) received via
/// [`driver::record_stream::RecordStream::consume()`].
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

mod private {
    // Trait to prevent traits from being implemented outside of this crate.
    pub trait Sealed {}
}

#[cfg(feature = "_internal_testkit_backend")]
pub mod testkit_backend;
