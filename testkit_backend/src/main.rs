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

mod testkit_backend;

use testkit_backend::start_server;

fn main() {
    env_logger::builder()
        .target(env_logger::Target::Stdout)
        .format_timestamp_millis()
        .format_module_path(false)
        .format_target(false)
        .init();
    start_server()
}
