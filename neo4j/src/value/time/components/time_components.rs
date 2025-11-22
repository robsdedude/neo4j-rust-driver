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

// imports for docs
#[allow(unused)]
use super::super::{LocalTime, Time};

use super::{DateComponents, DateTimeComponents};

pub(super) const SECONDS_PER_DAY: i64 = 86_400;

/// Helper struct to build or extract time components.
///
/// The struct does not perform any validation of the contained values.
///
/// See
///  * [`Time::from_utc_components`], [`Time::to_utc_components`]
///  * [`LocalTime::from_components`], [`LocalTime::to_components`]
///
/// # Example
/// ```
/// use neo4j::value::time::TimeComponents;
///
/// let components_1 = TimeComponents::from_hms_nano(18, 30, 10, 500_000_000);
/// let components_2 = TimeComponents {
///     hour: 18,
///     min: 30,
///     sec: 10,
///     nano: 500_000_000,
/// };
/// assert_eq!(components_1, components_2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeComponents {
    pub hour: u8,
    pub min: u8,
    pub sec: u8,
    pub nano: u32,
}

impl Default for TimeComponents {
    /// Create `TimeComponents` 00:00:00 (midnight).
    fn default() -> Self {
        Self {
            hour: 0,
            min: 0,
            sec: 0,
            nano: 0,
        }
    }
}

impl TimeComponents {
    pub(crate) fn from_nanos_since_midnight(nanos: i64) -> Self {
        let nano = nanos % 1_000_000_000;
        let secs = nanos / 1_000_000_000;
        let sec = secs % 60;
        let mins = secs / 60;
        let min = mins % 60;
        let hour = mins / 60;
        debug_assert_eq!(hour as u8 as i64, hour);
        debug_assert_eq!(min as u8 as i64, min);
        debug_assert_eq!(sec as u8 as i64, sec);
        debug_assert_eq!(nano as u32 as i64, nano);
        Self::from_hms_nano(hour as u8, min as u8, sec as u8, nano as u32)
    }

    pub(crate) fn to_nanos_since_midnight(self) -> Option<i64> {
        if self.hour >= 24 || self.min >= 60 || self.sec >= 60 || self.nano >= 1_000_000_000 {
            return None;
        }
        let secs = i64::from(self.hour) * 3600 + i64::from(self.min) * 60 + i64::from(self.sec);
        Some(secs * 1_000_000_000 + i64::from(self.nano))
    }

    /// Create `TimeComponents` 00:00:00 (midnight).
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `TimeComponents` as (hour, minute, second) tuple.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::TimeComponents;
    ///
    /// let components = TimeComponents::from_hms_nano(12, 32, 42, 1337);
    /// let (hour, min, sec) = components.hms();
    ///
    /// assert_eq!(hour, 12);
    /// assert_eq!(min, 32);
    /// assert_eq!(sec, 42);
    /// ```
    pub fn hms(&self) -> (u8, u8, u8) {
        let Self {
            hour,
            min,
            sec,
            nano: _,
        } = *self;
        (hour, min, sec)
    }

    /// Return `TimeComponents` as (hour, minute, second, and nanosecond) tuple.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::TimeComponents;
    ///
    /// let components = TimeComponents::from_hms_nano(12, 32, 42, 1337);
    /// let (hour, min, sec, nano) = components.hms_nano();
    ///
    /// assert_eq!(hour, 12);
    /// assert_eq!(min, 32);
    /// assert_eq!(sec, 42);
    /// assert_eq!(nano, 1337);
    /// ```
    pub fn hms_nano(&self) -> (u8, u8, u8, u32) {
        let Self {
            hour,
            min,
            sec,
            nano,
        } = *self;
        (hour, min, sec, nano)
    }

    /// Create `TimeComponents` from the given hour, minute, and second.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::TimeComponents;
    ///
    /// let components = TimeComponents::from_hms(12, 32, 42);
    ///
    /// assert_eq!(components.hour, 12);
    /// assert_eq!(components.min, 32);
    /// assert_eq!(components.sec, 42);
    /// assert_eq!(components.nano, 0);
    /// ```
    pub fn from_hms(hour: u8, min: u8, sec: u8) -> Self {
        Self::from_hms_nano(hour, min, sec, 0)
    }

    /// Create `TimeComponents` from the given hour, minute, second, and nanosecond.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::TimeComponents;
    ///
    /// let components = TimeComponents::from_hms_nano(12, 32, 42, 123456789);
    ///
    /// assert_eq!(components.hour, 12);
    /// assert_eq!(components.min, 32);
    /// assert_eq!(components.sec, 42);
    /// assert_eq!(components.nano, 123456789);
    /// ```
    pub fn from_hms_nano(hour: u8, min: u8, sec: u8, nano: u32) -> Self {
        Self {
            hour,
            min,
            sec,
            nano,
        }
    }

    /// Set the hour, minute, and second of the `TimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::TimeComponents;
    ///
    /// let components = TimeComponents::new().with_hms(12, 32, 42);
    ///
    /// assert_eq!(components.hour, 12);
    /// assert_eq!(components.min, 32);
    /// assert_eq!(components.sec, 42);
    /// assert_eq!(components.nano, 0);
    /// ```
    pub fn with_hms(mut self, hour: u8, min: u8, sec: u8) -> Self {
        self.hour = hour;
        self.min = min;
        self.sec = sec;
        self
    }

    /// Set the hour, minute, second, and nanosecond of the `TimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::TimeComponents;
    ///
    /// let components = TimeComponents::new().with_hms_nano(12, 32, 42, 123456789);
    ///
    /// assert_eq!(components.hour, 12);
    /// assert_eq!(components.min, 32);
    /// assert_eq!(components.sec, 42);
    /// assert_eq!(components.nano, 123456789);
    /// ```
    pub fn with_hms_nano(mut self, hour: u8, min: u8, sec: u8, nano: u32) -> Self {
        self.hour = hour;
        self.min = min;
        self.sec = sec;
        self.nano = nano;
        self
    }

    /// Set the year, month, day
    /// turning the `TimeComponents` into `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, TimeComponents};
    ///
    /// let components: DateTimeComponents = TimeComponents::new().with_ymd(2023, 12, 8);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// ```
    pub fn with_ymd(self, year: i64, month: u8, day: u8) -> DateTimeComponents {
        let date = DateComponents::from_ymd(year, month, day);
        DateTimeComponents::combine(date, self)
    }
}
