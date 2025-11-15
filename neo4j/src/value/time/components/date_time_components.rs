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
use super::super::{DateTime, DateTimeFixed, LocalDateTime};

use super::date_components::DAYS_IN_400_YEARS;
use super::time_components::SECONDS_PER_DAY;
use super::{DateComponents, TimeComponents};

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
    pub year: i64,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub min: u8,
    pub sec: u8,
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
    pub(crate) fn from_unix_timestamp(secs: i64, nanos: u32) -> Self {
        debug_assert!(nanos < 1_000_000_000);
        let days = secs.div_euclid(SECONDS_PER_DAY);
        let date = DateComponents::from_unix_ordinal(days);
        let secs_time = secs.rem_euclid(SECONDS_PER_DAY);
        let nanos_time = secs_time * 1_000_000_000 + i64::from(nanos);
        let time = TimeComponents::from_nanos_since_midnight(nanos_time);
        Self::combine(date, time)
    }

    pub(crate) fn to_unix_timestamp(self) -> Option<(i64, u32)> {
        let nanos = self.time().to_nanos_since_midnight()?;
        let seconds = nanos / 1_000_000_000;
        let nanos = nanos % 1_000_000_000;
        debug_assert_eq!(nanos as u32 as i64, nanos);
        let nanos = nanos as u32;
        if self.year < 0 {
            let mut date = self.date();
            date.year += 400;
            let ordinal = date.to_unix_ordinal()?;
            let seconds = ordinal
                .checked_mul(SECONDS_PER_DAY)?
                .checked_add(seconds)?
                .checked_add(DAYS_IN_400_YEARS * SECONDS_PER_DAY)?;
            Some((seconds, nanos))
        } else {
            let ordinal = self.date().to_unix_ordinal()?;
            let seconds = ordinal.checked_mul(SECONDS_PER_DAY)?.checked_add(seconds)?;
            Some((seconds, nanos))
        }
    }

    /// Create `DateTimeComponents` for 1970-01-01T00:00:00 (UNIX epoch).
    pub fn new() -> Self {
        Self::default()
    }

    /// Combine `DateComponents` and `TimeComponents` into `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents, TimeComponents};
    ///
    /// let date = DateComponents::from_ymd(2023, 12, 8);
    /// let time = TimeComponents::from_hms_nano(14, 30, 15, 123456789);
    ///
    /// let date_time = DateTimeComponents::combine(date, time);
    ///
    /// assert_eq!(date_time.year, 2023);
    /// assert_eq!(date_time.month, 12);
    /// assert_eq!(date_time.day, 8);
    /// assert_eq!(date_time.hour, 14);
    /// assert_eq!(date_time.min, 30);
    /// assert_eq!(date_time.sec, 15);
    /// assert_eq!(date_time.nano, 123456789);
    /// ```
    pub fn combine(date: DateComponents, time: TimeComponents) -> Self {
        Self {
            year: date.year,
            month: date.month,
            day: date.day,
            hour: time.hour,
            min: time.min,
            sec: time.sec,
            nano: time.nano,
        }
    }

    /// Extract the `DateComponents` from the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents, TimeComponents};
    ///
    /// let date = DateComponents::from_ymd(2023, 12, 8);
    /// let time = TimeComponents::from_hms_nano(14, 30, 15, 123456789);
    /// let date_time = DateTimeComponents::combine(date, time);
    ///
    /// assert_eq!(date_time.date(), date);
    /// ```
    pub fn date(&self) -> DateComponents {
        DateComponents::from_ymd(self.year, self.month, self.day)
    }

    /// Extract the `TimeComponents` from the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents, TimeComponents};
    ///
    /// let date = DateComponents::from_ymd(2023, 12, 8);
    /// let time = TimeComponents::from_hms_nano(14, 30, 15, 123456789);
    /// let date_time = DateTimeComponents::combine(date, time);
    ///
    /// assert_eq!(date_time.time(), time);
    /// ```
    pub fn time(&self) -> TimeComponents {
        TimeComponents::from_hms_nano(self.hour, self.min, self.sec, self.nano)
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
    /// assert_eq!(components.hour, DateTimeComponents::default().hour);
    /// assert_eq!(components.min, DateTimeComponents::default().min);
    /// assert_eq!(components.sec, DateTimeComponents::default().sec);
    /// assert_eq!(components.nano, DateTimeComponents::default().nano);
    /// ```
    pub fn from_ymd(year: i64, month: u8, day: u8) -> Self {
        Self::default().with_ymd(year, month, day)
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
    /// assert_eq!(components.hour, DateTimeComponents::default().hour);
    /// assert_eq!(components.min, DateTimeComponents::default().min);
    /// assert_eq!(components.sec, DateTimeComponents::default().sec);
    /// assert_eq!(components.nano, DateTimeComponents::default().nano);
    /// ```
    pub fn with_ymd(mut self, year: i64, month: u8, day: u8) -> Self {
        self.year = year;
        self.month = month;
        self.day = day;
        self
    }

    /// Set the date from `DateComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents};
    ///
    /// let date = DateComponents::from_ymd(2023, 12, 8);
    /// let components = DateTimeComponents::new().with_date(date);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// assert_eq!(components.hour, DateTimeComponents::default().hour);
    /// assert_eq!(components.min, DateTimeComponents::default().min);
    /// assert_eq!(components.sec, DateTimeComponents::default().sec);
    /// assert_eq!(components.nano, DateTimeComponents::default().nano);
    /// ```
    pub fn with_date(self, date: DateComponents) -> Self {
        self.with_ymd(date.year, date.month, date.day)
    }

    /// Create `DateTimeComponents` from the given `DateComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents};
    ///
    /// let date = DateComponents::from_ymd(2023, 12, 8);
    /// let components = DateTimeComponents::from_date(date);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// assert_eq!(components.hour, DateTimeComponents::default().hour);
    /// assert_eq!(components.min, DateTimeComponents::default().min);
    /// assert_eq!(components.sec, DateTimeComponents::default().sec);
    /// assert_eq!(components.nano, DateTimeComponents::default().nano);
    /// ```
    pub fn from_date(date: DateComponents) -> Self {
        Self::default().with_date(date)
    }

    /// Create `DateTimeComponents` from the given hour, minute, and second.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::from_hms(1, 2, 3);
    ///
    /// assert_eq!(components.year, DateTimeComponents::default().year);
    /// assert_eq!(components.month, DateTimeComponents::default().month);
    /// assert_eq!(components.day, DateTimeComponents::default().day);
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, DateTimeComponents::default().nano);
    /// ```
    pub fn from_hms(hour: u8, min: u8, sec: u8) -> Self {
        Self::default().with_hms(hour, min, sec)
    }

    /// Set the hour, minute, and second of the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::new().with_hms(1, 2, 3);
    ///
    /// assert_eq!(components.year, DateTimeComponents::default().year);
    /// assert_eq!(components.month, DateTimeComponents::default().month);
    /// assert_eq!(components.day, DateTimeComponents::default().day);
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, DateTimeComponents::default().nano);
    /// ```
    pub fn with_hms(mut self, hour: u8, min: u8, sec: u8) -> Self {
        self.hour = hour;
        self.min = min;
        self.sec = sec;
        self
    }

    /// Create `DateTimeComponents` from the given hour, minute, second, and nanosecond.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::from_hms_nano(1, 2, 3, 4);
    ///
    /// assert_eq!(components.year, DateTimeComponents::default().year);
    /// assert_eq!(components.month, DateTimeComponents::default().month);
    /// assert_eq!(components.day, DateTimeComponents::default().day);
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 4);
    /// ```
    pub fn from_hms_nano(hour: u8, min: u8, sec: u8, nano: u32) -> Self {
        Self::default().with_hms_nano(hour, min, sec, nano)
    }

    /// Set the hour, minute, second, and nanosecond of the `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeComponents;
    ///
    /// let components = DateTimeComponents::new().with_hms_nano(1, 2, 3, 4);
    ///
    /// assert_eq!(components.year, DateTimeComponents::default().year);
    /// assert_eq!(components.month, DateTimeComponents::default().month);
    /// assert_eq!(components.day, DateTimeComponents::default().day);
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 4);
    /// ```
    pub fn with_hms_nano(mut self, hour: u8, min: u8, sec: u8, nano: u32) -> Self {
        self = self.with_hms(hour, min, sec);
        self.nano = nano;
        self
    }

    /// Set the time from `TimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, TimeComponents};
    ///
    /// let time = TimeComponents::from_hms_nano(1, 2, 3, 4);
    /// let components = DateTimeComponents::new().with_time(time);
    ///
    /// assert_eq!(components.year, DateTimeComponents::default().year);
    /// assert_eq!(components.month, DateTimeComponents::default().month);
    /// assert_eq!(components.day, DateTimeComponents::default().day);
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 4);
    /// ```
    pub fn with_time(self, time: TimeComponents) -> Self {
        self.with_hms_nano(time.hour, time.min, time.sec, time.nano)
    }

    /// Create `DateTimeComponents` from the given `TimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, TimeComponents};
    ///
    /// let time = TimeComponents::from_hms_nano(1, 2, 3, 4);
    /// let components = DateTimeComponents::from_time(time);
    ///
    /// assert_eq!(components.year, DateTimeComponents::default().year);
    /// assert_eq!(components.month, DateTimeComponents::default().month);
    /// assert_eq!(components.day, DateTimeComponents::default().day);
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 4);
    /// ```
    pub fn from_time(time: TimeComponents) -> Self {
        Self::default().with_time(time)
    }
}
