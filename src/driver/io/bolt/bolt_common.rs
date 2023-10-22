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

use crate::value::value_receive::{BrokenValueInner, ValueReceive};

pub(super) const TAG_2D_POINT: u8 = b'X';
pub(super) const TAG_3D_POINT: u8 = b'Y';
pub(super) const TAG_NODE: u8 = b'N';
pub(super) const TAG_RELATIONSHIP: u8 = b'R';
pub(super) const TAG_UNBOUND_RELATIONSHIP: u8 = b'r';
pub(super) const TAG_PATH: u8 = b'P';
pub(super) const TAG_DATE: u8 = b'D';
pub(super) const TAG_TIME: u8 = b'T';
pub(super) const TAG_LOCAL_TIME: u8 = b't';
pub(super) const TAG_DATE_TIME: u8 = b'I';
pub(super) const TAG_LEGACY_DATE_TIME: u8 = b'F';
pub(super) const TAG_DATE_TIME_ZONE_ID: u8 = b'i';
pub(super) const TAG_LEGACY_DATE_TIME_ZONE_ID: u8 = b'f';
pub(super) const TAG_LOCAL_DATE_TIME: u8 = b'd';
pub(super) const TAG_DURATION: u8 = b'E';

macro_rules! value_as {
    ($variant:ident, $value:expr, $name:literal, $type_name:literal $($format_arg:tt)*) => {
        match $value {
            ValueReceive::$variant(i) => i,
            v => {
                return invalid_struct(format!(
                    concat!(
                        "expected ",
                        $name,
                        " to be an ",
                        $type_name,
                        ", found {0:?}"
                    ),
                    v
                    $($format_arg)*
                ));
            }
        }
    };
}

macro_rules! as_int {
    ($value:expr, $name:literal $($format_arg:tt)*) => {
        value_as!(Integer, $value, $name, "integer" $($format_arg)*)
    };
}

macro_rules! as_float {
    ($value:expr, $name:literal $($format_arg:tt)*) => {
        value_as!(Float, $value, $name, "float" $($format_arg)*)
    };
}

macro_rules! as_string {
    ($value:expr, $name:literal $($format_arg:tt)*) => {
        value_as!(String, $value, $name, "string" $($format_arg)*)
    };
}

macro_rules! as_map {
    ($value:expr, $name:literal $($format_arg:tt)*) => {
        value_as!(Map, $value, $name, "map" $($format_arg)*)
    };
}

macro_rules! as_vec {
    ($value:expr, $name:literal $($format_arg:tt)*) => {
        value_as!(List, $value, $name, "list" $($format_arg)*)
    };
}

macro_rules! as_node {
    ($value:expr, $name:literal $($format_arg:tt)*) => {
        value_as!(Node, $value, $name, "Node" $($format_arg)*)
    };
}

#[inline]
pub(super) fn invalid_struct(reason: impl Into<String>) -> ValueReceive {
    let reason = reason.into();
    ValueReceive::BrokenValue(BrokenValueInner::InvalidStruct { reason }.into())
}

#[inline]
pub(super) fn failed_struct(reason: impl Into<String>) -> ValueReceive {
    ValueReceive::BrokenValue(BrokenValueInner::Reason(reason.into()).into())
}
