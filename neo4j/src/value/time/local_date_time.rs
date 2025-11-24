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

use super::DateTimeComponents;

/// Represents a date (year, month, day) and time (hour, minute, second, nanoseconds) without
/// time zone information in the DBMS.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalDateTime {
    secs: i64,
    nanos: u32,
}

impl LocalDateTime {
    /// Make a new `LocalDateTime` from the given timestamp in non-leap seconds and nanoseconds
    /// elapsed since UNIX epoch (1970-01-01T00:00:00) assuming a UTC-like local time.
    /// UTC-like meaning that the local date time is treated as if it were in UTC.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The value for `nanosecond` is invalid (must be less than `1_000_000_000`).
    /// - `seconds` is out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::LocalDateTime;
    ///
    /// let dt = LocalDateTime::from_timestamp(0, 0).unwrap();
    /// assert_eq!(dt.timestamp(), (0, 0));
    ///
    /// let dt = LocalDateTime::from_timestamp(2, 1_000_000_001);
    /// assert!(dt.is_none());
    /// ```
    pub fn from_timestamp(secs: i64, nanos: u32) -> Option<Self> {
        if nanos >= 1_000_000_000 {
            return None;
        }
        Some(Self { secs, nanos })
    }

    /// Return the timestamp of this `LocalDateTime` as non-leap seconds and nanoseconds elapsed
    /// since UNIX epoch (1970-01-01T00:00:00) assuming a UTC-like local time.
    /// UTC-like meaning that the local date time is treated as if it were in UTC.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, LocalDateTime};
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 1, 1).with_hms_nano(0, 0, 12, 34);
    /// let dt = LocalDateTime::from_components(components).unwrap();
    /// assert_eq!(dt.timestamp(), (12, 34));
    /// ```
    pub fn timestamp(&self) -> (i64, u32) {
        (self.secs, self.nanos)
    }

    /// Make a new `LocalDateTime` from [`DateTimeComponents`].
    ///
    /// # Errors
    /// Returns `None` if
    /// - The `components` are invalid. E.g.,
    ///   - `nanoseconds` must be less than `1_000_000_000`.
    ///     I.e., leap seconds are not supported.
    ///   - `components` represents a non-existent date like 2023-04-31.
    ///   - Any value is out of range (e.g., `month` > 12).
    /// - The represented `LocalDateTime` is out of range for the computation or storage.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, LocalDateTime};
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 1, 1);
    /// let dt = LocalDateTime::from_components(components).unwrap();
    /// assert_eq!(dt.timestamp(), (0, 0));
    ///
    /// let components = DateTimeComponents::from_ymd(i64::MAX, 1, 1);
    /// assert!(LocalDateTime::from_components(components).is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(i64::MIN, 1, 1);
    /// assert!(LocalDateTime::from_components(components).is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 0, 1);
    /// assert!(LocalDateTime::from_components(components).is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(-3, 2, 29); // 4 BCE is not a leap year
    /// assert!(LocalDateTime::from_components(components).is_none());
    /// ```
    pub fn from_components(components: DateTimeComponents) -> Option<Self> {
        let (secs, nanos) = components.to_unix_timestamp()?;
        Some(Self { secs, nanos })
    }

    /// Return the `LocalDateTime` as [`DateTimeComponents`].
    ///
    /// # Errors
    /// Returns `None` if
    /// - The represented `LocalDateTime` is out of range for the computation.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, LocalDateTime};
    ///
    /// let dt = LocalDateTime::from_timestamp(0, 0).unwrap();
    /// let components = dt.to_components().unwrap();
    /// assert_eq!(components, DateTimeComponents::from_ymd(1970, 1, 1))
    /// ```
    pub fn to_components(self) -> Option<DateTimeComponents> {
        let Self { secs, nanos } = self;
        Some(DateTimeComponents::from_unix_timestamp(secs, nanos))
    }
}

#[cfg(feature = "chrono_0_4")]
mod chrono_0_4_impl {
    use super::super::{local_date_time_from_timestamp, ChronoConversionError};
    use super::*;

    use chrono_0_4 as chrono;

    impl LocalDateTime {
        /// Convert a [`chrono::NaiveDateTime`] (from `chrono` version 0.4)
        /// into a `LocalDateTime`.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::NaiveDateTime`] is out of range for the `LocalDateTime` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        /// - The [`chrono::NaiveDateTime`] represents a leap second.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveDate;
        /// use neo4j::value::time::LocalDateTime;
        ///
        /// let chrono_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        ///
        /// let chrono_dt = chrono_date.and_hms_nano_opt(0, 0, 42, 123).unwrap();
        /// let dt = LocalDateTime::from_chrono_0_4(chrono_dt).unwrap();
        /// let (sec, nano) = dt.timestamp();
        /// assert_eq!(sec, 42);
        /// assert_eq!(nano, 123);
        ///
        /// let chrono_dt = chrono_date
        ///     .and_hms_nano_opt(0, 0, 59, 1_999_999_999)
        ///     .unwrap();
        /// let dt = LocalDateTime::from_chrono_0_4(chrono_dt);
        /// assert!(dt.is_none()); // Leap seconds not supported
        /// ```
        pub fn from_chrono_0_4(date_time: chrono::NaiveDateTime) -> Option<Self> {
            let sec = date_time.and_utc().timestamp();
            let nano = date_time.and_utc().timestamp_subsec_nanos();
            if nano >= 1_000_000_000 {
                // Leap seconds are not supported
                return None;
            }
            Self::from_timestamp(sec, nano)
        }

        /// Convert the `LocalDateTime` into a [`chrono::NaiveDateTime`]
        /// with a [`chrono::FixedOffset`] using `chrono` 0.4.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `LocalDateTime` is out of range for the [`chrono`] crate
        ///   version 0.4.  
        ///   The exact range valid is defined by the `chrono` crate.
        ///
        ///  # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveDate;
        /// use neo4j::value::time::{Date, DateComponents};
        ///
        /// let date = Date::from_components(DateComponents::from_ymd(2023, 12, 8)).unwrap();
        /// let chrono_date = date.to_chrono_0_4().unwrap();
        /// assert_eq!(chrono_date, NaiveDate::from_ymd_opt(2023, 12, 8).unwrap());
        /// ```
        pub fn to_chrono_0_4(self) -> Option<chrono::NaiveDateTime> {
            let Self {
                secs: sec,
                nanos: nano,
            } = self;
            local_date_time_from_timestamp(sec, nano)
        }
    }

    impl TryFrom<chrono::NaiveDateTime> for LocalDateTime {
        type Error = ChronoConversionError;

        /// See [`LocalDateTime::from_chrono_0_4`].
        fn try_from(value: chrono::NaiveDateTime) -> Result<Self, Self::Error> {
            LocalDateTime::from_chrono_0_4(value).ok_or(ChronoConversionError {
                source_type: "chrono::NaiveDateTime",
                target_type: "LocalDateTime",
            })
        }
    }

    impl TryFrom<LocalDateTime> for chrono::NaiveDateTime {
        type Error = ChronoConversionError;

        /// See [`LocalDateTime::to_chrono_0_4`].
        fn try_from(value: LocalDateTime) -> Result<Self, Self::Error> {
            value.to_chrono_0_4().ok_or(ChronoConversionError {
                source_type: "LocalDateTime",
                target_type: "chrono::NaiveDateTime",
            })
        }
    }
}
