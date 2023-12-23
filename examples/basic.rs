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

use std::sync::Arc;

use neo4j::address::Address;
use neo4j::driver::auth::AuthToken;
use neo4j::driver::{ConnectionConfig, Driver, DriverConfig, RoutingControl};
use neo4j::retry::ExponentialBackoff;
use neo4j::{value_map, ValueReceive};

const HOST: &str = "localhost";
const PORT: u16 = 7687;
const USER: &str = "neo4j";
const PASSWORD: &str = "pass";
const DATABASE: &str = "neo4j";

fn main() {
    let database = Arc::new(String::from(DATABASE));
    let address = Address::from((HOST, PORT));
    let auth_token = AuthToken::new_basic_auth(USER, PASSWORD);
    let driver = Driver::new(
        ConnectionConfig::new(address),
        DriverConfig::new().with_auth(Arc::new(auth_token)),
    );

    let result = driver
        .execute_query("RETURN $x AS x")
        .with_database(database)
        .with_routing_control(RoutingControl::Read)
        .with_parameters(value_map!({"x": 123}))
        .run_with_retry(ExponentialBackoff::default());
    println!("{:?}", result);

    let result = result.unwrap();
    assert_eq!(result.records.len(), 1);
    for mut record in result.records {
        assert_eq!(record.values().count(), 1);
        assert_eq!(record.take_value("x"), Some(ValueReceive::Integer(123)));
    }
}
