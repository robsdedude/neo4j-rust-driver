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

use chrono::TimeZone;

use super::{chrono, local_date_time_from_timestamp, DateTimeComponents};

/// Represents a date (year, month, day) and time (hour, minute, second, nanoseconds) within a time
/// zone with fixed UTC offset in the DBMS.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTimeFixed {
    secs: i64,
    nanos: u32,
    utc_offset: i32,
}

impl DateTimeFixed {
    pub(crate) fn from_chrono(date_time: &chrono::DateTime<chrono::FixedOffset>) -> Option<Self> {
        let sec = date_time.timestamp();
        let nano = date_time.timestamp_subsec_nanos();
        if nano >= 1_000_000_000 {
            // Leap seconds are not supported
            return None;
        }
        let tz_offset = date_time.offset().local_minus_utc();
        Self::from_utc_timestamp(sec, nano, tz_offset)
    }

    pub(crate) fn to_chrono(self) -> Option<chrono::DateTime<chrono::FixedOffset>> {
        let Self {
            secs,
            nanos,
            utc_offset,
        } = self;
        let tz = chrono::FixedOffset::east_opt(utc_offset)?;
        let dt = local_date_time_from_timestamp(secs, nanos)?;
        Some(tz.from_utc_datetime(&dt))
    }

    /// Make a new `DateTimeFixed` from the given timestamp in non-leap seconds and nanoseconds
    /// elapsed since UNIX epoch in UTC (1970-01-01T00:00:00Z) and a fixed UTC offset.
    /// The offset is given in seconds east of UTC, i.e., the number of seconds to add to the
    /// timestamp to convert from UTC to local time.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The value for `nanoseconds` is invalid (must be less than `1_000_000_000`).
    /// - `seconds` or `tz_offset` is out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeFixed;
    ///
    /// // Create DateTimeFixed at 1970-01-01T00:00:00Z in Etc/GMT-2 timezone.
    /// // i.e., 1970-01-01T02:00:00+02:00 in local time.
    /// let dt = DateTimeFixed::from_utc_timestamp(0, 0, -3600).unwrap();
    /// assert_eq!(dt.utc_timestamp(), (0, 0));
    ///
    /// let dt = DateTimeFixed::from_utc_timestamp(2, 1_000_000_001, -3600);
    /// assert!(dt.is_none());
    /// ```
    pub fn from_utc_timestamp(secs: i64, nanos: u32, utc_offset: i32) -> Option<Self> {
        if nanos >= 1_000_000_000 {
            return None;
        }
        Some(Self {
            secs,
            nanos,
            utc_offset,
        })
    }

    /// Return the timestamp of this `DateTimeFixed` as non-leap seconds and nanoseconds elapsed
    /// since UNIX epoch in UTC (1970-01-01T00:00:00Z).
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, DateTimeFixed};
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 1, 1).with_hms_nano(0, 0, 12, 34);
    /// let dt = DateTimeFixed::from_utc_components(components, 3600).unwrap();
    /// assert_eq!(dt.utc_timestamp(), (12, 34));
    /// ```
    pub fn utc_timestamp(&self) -> (i64, u32) {
        (self.secs, self.nanos)
    }

    /// Return the offset from UTC of this `DateTimeFixed` in seconds.
    /// The offset is the number of seconds to add to the timestamp to convert from UTC to local
    /// time.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTimeFixed;
    ///
    /// let dt = DateTimeFixed::from_utc_timestamp(0, 0, 12345).unwrap();
    /// assert_eq!(dt.utc_offset(), 12345);
    /// ```
    pub fn utc_offset(&self) -> i32 {
        self.utc_offset
    }

    /// Make a new `DateTimeFixed` from the given UTC date and time components along with a fixed
    /// UTC offset.
    /// The offset is given in seconds east of UTC, i.e., the number of seconds to add to the
    /// timestamp to convert from UTC to local time.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The `components` are invalid. E.g.,
    ///   - `nanoseconds` must be less than `1_000_000_000`.
    ///     I.e., leap seconds are not supported.
    ///   - `components` represents a non-existent date like 2023-04-31.
    ///   - Any value is out of range (e.g., `month` > 12).
    /// - The represented `DateTimeFixed` is out of range for the computation or storage.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, DateTimeFixed};
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 1, 1);
    /// let dt = DateTimeFixed::from_utc_components(components, 0).unwrap();
    /// assert_eq!(dt.utc_timestamp(), (0, 0));
    /// assert_eq!(dt.utc_offset(), 0);
    ///
    /// let components = DateTimeComponents::from_ymd(i32::MAX, 1, 1);
    /// assert!(DateTimeFixed::from_utc_components(components, 0).is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(i32::MIN, 1, 1);
    /// assert!(DateTimeFixed::from_utc_components(components, 0).is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 0, 1);
    /// assert!(DateTimeFixed::from_utc_components(components, 0).is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(-3, 2, 29); // 4 BCE is not a leap year
    /// assert!(DateTimeFixed::from_utc_components(components, 0).is_none());
    /// ```
    pub fn from_utc_components(components: DateTimeComponents, tz_offset: i32) -> Option<Self> {
        let dt_utc = components.to_chrono()?.and_utc();
        let sec = dt_utc.timestamp();
        let nano = dt_utc.timestamp_subsec_nanos();
        if components.nano >= 1_000_000_000 {
            // Leap seconds are not supported
            return None;
        }
        Some(Self {
            secs: sec,
            nanos: nano,
            utc_offset: tz_offset,
        })
    }

    /// Return the `DateTimeFixed` as UTC date and time components along with the fixed
    /// UTC offset.
    /// The offset is given in seconds east of UTC, i.e., the number of seconds to add to the
    /// timestamp to convert from UTC to local time.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The represented `DateTimeFixed` is out of range for the computation.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTimeComponents, DateTimeFixed};
    ///
    /// let dt = DateTimeFixed::from_utc_timestamp(0, 0, 12345).unwrap();
    /// let (components, tz_offset) = dt.to_utc_components().unwrap();
    /// assert_eq!(tz_offset, 12345);
    /// assert_eq!(components, DateTimeComponents::from_ymd(1970, 1, 1))
    /// ```
    pub fn to_utc_components(self) -> Option<(DateTimeComponents, i32)> {
        let Self {
            secs: sec,
            nanos: nano,
            utc_offset: tz_offset,
        } = self;
        let dt_utc = local_date_time_from_timestamp(sec, nano)?;
        let components = DateTimeComponents::from_chrono(&dt_utc)?;
        Some((components, tz_offset))
    }
}

#[cfg(feature = "chrono_0_4")]
mod chrono_0_4_impl {
    use super::super::ChronoConversionError;
    use super::*;

    use chrono_0_4 as chrono;

    impl DateTimeFixed {
        /// Convert a [`chrono::DateTime`] (from `chrono` version 0.4)
        /// with a [`chrono::FixedOffset`] timezone (from `chrono` version 0.4)
        /// into a `DateTimeFixed`.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::DateTime`] is out of range for the `DateTimeFixed` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        /// - The [`chrono::DateTime`] represents a leap second.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::{FixedOffset, TimeZone, Timelike};
        /// use neo4j::value::time::DateTimeFixed;
        ///
        /// let tz = FixedOffset::east_opt(-42).unwrap();
        /// let chrono_dt = tz
        ///     .with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
        ///     .single()
        ///     .unwrap()
        ///     .with_nanosecond(123)
        ///     .unwrap();
        /// let dt = DateTimeFixed::from_chrono_0_4(&chrono_dt).unwrap();
        /// let (sec, nano) = dt.utc_timestamp();
        /// assert_eq!(sec, 42);
        /// assert_eq!(nano, 123);
        /// assert_eq!(dt.utc_offset(), -42);
        ///
        /// let chrono_dt = tz
        ///     .with_ymd_and_hms(1970, 1, 1, 0, 0, 59)
        ///     .single()
        ///     .unwrap()
        ///     .with_nanosecond(1_000_000_001)
        ///     .unwrap();
        /// let dt = DateTimeFixed::from_chrono_0_4(&chrono_dt);
        /// assert!(dt.is_none()); // Leap seconds not supported
        /// ```
        pub fn from_chrono_0_4(date_time: &chrono::DateTime<chrono::FixedOffset>) -> Option<Self> {
            Self::from_chrono(date_time)
        }

        /// Convert the `DateTimeFixed` into a [`chrono::DateTime`]
        /// with a [`chrono::FixedOffset`] using `chrono` 0.4.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `DateTimeFixed` is out of range for the [`chrono`] crate
        ///   version 0.4.  
        ///   The exact range valid is defined by the `chrono` crate.
        /// - The utc offset of `DateTimeFixed` is out of range for [`chrono::FixedOffset`].  
        ///   The exact range valid is defined by the `chrono` crate.
        ///
        ///  # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveDate;
        /// use neo4j::value::time::Date;
        ///
        /// let date = Date::from_ymd(2023, 12, 8).unwrap();
        /// let chrono_date = date.to_chrono_0_4().unwrap();
        /// assert_eq!(chrono_date, NaiveDate::from_ymd_opt(2023, 12, 8).unwrap());
        /// ```
        pub fn to_chrono_0_4(self) -> Option<chrono::DateTime<chrono::FixedOffset>> {
            self.to_chrono()
        }
    }

    impl TryFrom<&chrono::DateTime<chrono::FixedOffset>> for DateTimeFixed {
        type Error = ChronoConversionError;

        /// See [`DateTimeFixed::from_chrono_0_4`].
        fn try_from(value: &chrono::DateTime<chrono::FixedOffset>) -> Result<Self, Self::Error> {
            DateTimeFixed::from_chrono_0_4(value).ok_or(ChronoConversionError {
                source_type: "chrono::DateTime<chrono::FixedOffset>",
                target_type: "DateTimeFixed",
            })
        }
    }

    impl TryFrom<&DateTimeFixed> for chrono::DateTime<chrono::FixedOffset> {
        type Error = ChronoConversionError;

        /// See [`DateTimeFixed::to_chrono_0_4`].
        fn try_from(value: &DateTimeFixed) -> Result<Self, Self::Error> {
            value.to_chrono_0_4().ok_or(ChronoConversionError {
                source_type: "DateTimeFixed",
                target_type: "chrono::DateTime<chrono::FixedOffset>",
            })
        }
    }
}
