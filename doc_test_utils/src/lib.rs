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

use std::env;
use std::panic::{catch_unwind, resume_unwind, UnwindSafe};
use std::sync::Arc;

use named_lock::NamedLock;

use neo4j::address::Address;
use neo4j::driver::auth::AuthToken;
use neo4j::driver::{ConnectionConfig, Driver, DriverConfig};
use neo4j::retry::ExponentialBackoff;
use neo4j::session::{Session, SessionConfig};
use neo4j::transaction::Transaction;
use neo4j::Result as Neo4jResult;

pub fn get_host() -> String {
    env::var("TEST_NEO4J_HOST").expect("env var TEST_NEO4J_HOST not set")
}

pub fn get_port() -> u16 {
    env::var("TEST_NEO4J_PORT")
        .expect("env var TEST_NEO4J_PORT not set")
        .parse()
        .expect("TEST_NEO4J_PORT is not a valid port")
}

pub fn get_address() -> Address {
    let host = get_host();
    let port = get_port();
    (host, port).into()
}

pub fn get_user() -> String {
    env::var("TEST_NEO4J_USER").expect("env var TEST_NEO4J_USER not set")
}

pub fn get_password() -> String {
    env::var("TEST_NEO4J_PASS").expect("env var TEST_NEO4J_PASS not set")
}

pub fn get_auth_token() -> AuthToken {
    let user = get_user();
    let password = get_password();
    AuthToken::new_basic_auth(user, password)
}

pub fn get_driver() -> Driver {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();

    let driver = neo4j::driver::Driver::new(
        ConnectionConfig::new(get_address()),
        DriverConfig::new().with_auth(Arc::new(get_auth_token())),
    );

    driver
        .execute_query("MATCH (n) DETACH DELETE n")
        .run_with_retry(ExponentialBackoff::default())
        .unwrap();

    driver
}

pub fn get_session(driver: &Driver) -> Session {
    driver.session(
        SessionConfig::new()
            .with_bookmark_manager(driver.execute_query_bookmark_manager())
            .with_database(Arc::new(String::from("neo4j"))),
    )
}

pub fn with_transaction<R>(example: impl FnMut(Transaction) -> Neo4jResult<R>) {
    let driver = get_driver();
    let mut session = get_session(&driver);
    session
        .transaction()
        .run_with_retry(ExponentialBackoff::default(), example)
        .unwrap();
}

pub fn db_exclusive(work: impl FnOnce() + UnwindSafe) {
    let lock = NamedLock::create("neo4j_rust_driver_docs_test_lock").unwrap();
    let guard = lock.lock().unwrap();

    let res = catch_unwind(work);

    drop(guard);

    if let Err(err) = res {
        resume_unwind(err)
    }
}
