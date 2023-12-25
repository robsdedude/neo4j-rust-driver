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

/// until (type_name_of_val)[https://doc.rust-lang.org/std/any/fn.type_name_of_val.html]
/// becomes stable, we'll do it on our own =)
pub(crate) fn get_type_name<T>(_: T) -> &'static str {
    type_name::<T>()
}

pub(crate) fn truncate_string(string: &str, start: usize, end: usize) -> &str {
    let mut chars = string.chars();
    for _ in 0..start {
        if chars.next().is_none() {
            break;
        }
    }
    for _ in 0..end {
        if chars.next_back().is_none() {
            break;
        }
    }
    chars.as_str()
}

pub(crate) enum RefContainer<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T> AsRef<T> for RefContainer<'a, T> {
    fn as_ref(&self) -> &T {
        match self {
            RefContainer::Owned(t) => t,
            RefContainer::Borrowed(t) => t,
        }
    }
}
