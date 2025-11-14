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

use chrono::{Datelike, Timelike};

// imports for docs
#[allow(unused)]
use super::{DateTime, DateTimeFixed, LocalDateTime};

use super::chrono;

/// Helper struct to build or extract date and time components.
///
/// The struct does not perform any validation of the contained values.
///
/// See
///  * [`DateTime::from_utc_components`], [`DateTime::to_utc_components`]
///  * [`DateTimeFixed::from_utc_components`], [`DateTimeFixed::to_utc_components`]
///  * [`LocalDateTime::from_components`], [`LocalDateTime::to_components`]
///
/// # Example
/// ```
/// use neo4j::value::time::DateTimeComponents;
///
/// let components_1 =
///     DateTimeComponents::from_ymd(2000, 12, 24).with_hms_nano(18, 30, 10, 500_000_000);
/// let components_2 = DateTimeComponents {
///     year: 2000,
///     month: 12,
///     day: 24,
///     hour: 18,
///     min: 30,
///     sec: 10,
///     nano: 500_000_000,
/// };
/// assert_eq!(components_1, components_2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTimeComponents {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub min: u32,
    pub sec: u32,
    pub nano: u32,
}

impl Default for DateTimeComponents {
    /// Create `DateTimeComponents` for 1970-01-01T00:00:00 (UNIX epoch).
    fn default() -> Self {
        Self {
            year: 1970,
            month: 1,
            day: 1,
            hour: 0,
            min: 0,
            sec: 0,
            nano: 0,
        }
    }
}

impl DateTimeComponents {
    pub(crate) fn from_chrono(date_time: &chrono::NaiveDateTime) -> Option<Self> {
        Some(Self {
            year: date_time.year(),
            month: date_time.month(),
            day: date_time.day(),
            hour: date_time.hour(),
            min: date_time.minute(),
            sec: date_time.second(),
            nano: date_time.nanosecond(),
        })
    }

    pub(crate) fn to_chrono(self) -> Option<chrono::NaiveDateTime> {
        let DateTimeComponents {
            year,
            month,
            day,
            hour,
            min,
            sec,
            nano,
        } = self;
        let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
        date.and_hms_nano_opt(hour, min, sec, nano)
    }

    /// Create `DateTimeComponents` for 1970-01-01T00:00:00 (UNIX epoch).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create `DateTimeComponents` from the given year, month, and day.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::from_ymd(2023, 12, 8);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// assert_eq!(components.hour, 0);
    /// assert_eq!(components.min, 0);
    /// assert_eq!(components.sec, 0);
    /// assert_eq!(components.nano, 0);
    /// ```
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Self {
        Self {
            year,
            month,
            day,
            ..Self::default()
        }
    }

    /// Set the year, month, and day of the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::new().with_ymd(2023, 12, 8);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// ```
    pub fn with_ymd(mut self, year: i32, month: u32, day: u32) -> Self {
        self.year = year;
        self.month = month;
        self.day = day;
        self
    }

    /// Set the hour, minute, and second of the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::new().with_hms(1, 2, 3);
    ///
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// ```
    pub fn with_hms(mut self, hour: u32, min: u32, sec: u32) -> Self {
        self.hour = hour;
        self.min = min;
        self.sec = sec;
        self
    }

    /// Set the hour, minute, second, and nanosecond of the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::new().with_hms_nano(1, 2, 3, 4);
    ///
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 4);
    /// ```
    pub fn with_hms_nano(mut self, hour: u32, min: u32, sec: u32, nano: u32) -> Self {
        self = self.with_hms(hour, min, sec);
        self.nano = nano;
        self
    }
}
