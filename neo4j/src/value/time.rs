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

//! Temporal types to be exchanged with the DBMS.
//!
//! # Limitations
//! Note that neo4j's temporal types do *not* encode leap seconds.
//!
//! If a value can be instantiated is no guarantee that it is valid according to the DBMS, which is
//! free to reject values considered invalid by returning an error.
//!
//! Vice versa, not all values valid in the DBMS are necessarily representable by the types in this
//! crate. In such case, a [`ValueReceive::BrokenValue`] is returned instead.
//!
//! Arithmetic operations on temporal types are not implemented in this crate.
//! This is left to the user or a higher-level crate.
//! To help bridge the gap, some types support conversion to/from `chrono` types given the right
//! features are enabled.
//! Note, however, that the mapping isn't 1-to-1 as `chrono` usually provides smaller valid value
//! ranges.

use thiserror::Error;

// imports for docs
#[allow(unused)]
use crate::value::value_receive::ValueReceive;

pub(crate) mod date;
pub(crate) mod date_time;
mod date_time_components;
pub(crate) mod date_time_fixed;
pub(crate) mod duration;
pub(crate) mod local_date_time;
pub(crate) mod local_time;
#[allow(clippy::module_inception)]
pub(crate) mod time;

pub(crate) use chrono_0_4 as chrono;
pub(crate) use chrono_tz_0_10 as chrono_tz;

pub use date::Date;
pub use date_time::DateTime;
pub use date_time_components::DateTimeComponents;
pub use date_time_fixed::DateTimeFixed;
pub use duration::Duration;
pub use local_date_time::LocalDateTime;
pub use local_time::LocalTime;
pub use time::Time;

pub(crate) fn local_date_time_from_timestamp(sec: i64, nano: u32) -> Option<chrono::NaiveDateTime> {
    chrono::DateTime::from_timestamp(sec, nano).map(|dt| dt.naive_utc())
}

/// Error indicating that a conversion between chrono and neo4j temporal types failed
/// because the source value is not representable in the target type.
#[derive(Debug, Clone, Error)]
#[error("{source_type} is out of range for {target_type}")]
pub struct ChronoConversionError {
    source_type: &'static str,
    target_type: &'static str,
}
