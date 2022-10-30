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

use std::any::type_name;

macro_rules! map {
    () => {std::collections::HashMap::new()};
    ( $($key:expr => $value:expr),* ) => {
        {
            let mut m = std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )*
            m
        }
    };
}

pub(crate) use map;

/// until (type_name_of_val)[https://doc.rust-lang.org/std/any/fn.type_name_of_val.html]
/// becomes stable, we'll do it on our own =)
pub fn get_type_name<T>(_: T) -> &'static str {
    type_name::<T>()
}
