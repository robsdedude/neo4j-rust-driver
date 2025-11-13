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

use super::local_time::LocalTime;

/// Represents a time value (hour, minute, second, nanosecond) within a time
/// zone with fixed UTC offset in the DBMS.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Time {
    time: LocalTime,
    utc_offset: i32,
}

impl Time {
    /// Makes a new `Time` from nanoseconds since midnight and a fixed UTC offset.
    /// The offset is given in seconds east of UTC, i.e., the number of seconds to add to the
    /// timestamp to convert from UTC to local time.
    ///
    /// # Errors
    /// Returns `None` if:
    /// - The value for `nanoseconds` is invalid (must be less than `1_000_000_000`).
    ///   `seconds` or `tz_offset` is out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Time;
    ///
    /// assert!(Time::from_nanos_since_midnight(0, 0).is_some());
    /// // 23:59:59.999999999+07:30
    /// assert!(Time::from_nanos_since_midnight(24 * 60 * 60 * 1_000_000_000 - 1, 27000).is_some());
    /// assert!(Time::from_nanos_since_midnight(u64::MAX, 0).is_none());
    /// ```
    pub fn from_nanos_since_midnight(nanos: u64, utc_offset: i32) -> Option<Self> {
        let time = LocalTime::from_nanos_since_midnight(nanos)?;
        Some(Self { time, utc_offset })
    }

    /// Return the `Time`'s nanoseconds since midnight.
    /// This is independent of the UTC offset.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Time;
    ///
    /// let time = Time::from_nanos_since_midnight(12345678912345, -3600).unwrap();
    /// assert_eq!(time.nanos_since_midnight(), 12345678912345);
    /// ```
    pub fn nanos_since_midnight(&self) -> u64 {
        self.time.nanos_since_midnight()
    }

    pub(crate) fn nanos(&self) -> i64 {
        self.time.nanos()
    }

    /// Return the offset from UTC of this `Time` in seconds.
    /// The offset is the number of seconds to add to the timestamp to convert from UTC to local
    /// time.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Time;
    ///
    /// let time = Time::from_nanos_since_midnight(12345678912345, -1234).unwrap();
    /// assert_eq!(time.utc_offset(), -1234);
    ///
    /// let time = Time::from_nanos_since_midnight(12345678912345, 4321).unwrap();
    /// assert_eq!(time.utc_offset(), 4321);
    /// ```
    pub fn utc_offset(&self) -> i32 {
        self.utc_offset
    }

    /// Makes a new `Time` from the hour, minute, second, nanosecond.
    ///
    /// # Errors
    /// Returns `None` if:
    /// - The value for `nanoseconds` is invalid (must be less than `1_000_000_000`).
    /// - The value for `hour`, `minute`, or `second` is invalid.
    /// - The resulting `Time` is out of range for the computation or storage.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    ///
    /// ```
    /// use neo4j::value::time::Time;
    ///
    /// assert!(Time::from_hms_nano_offset(0, 0, 0, 0, 0).is_some());
    /// assert!(Time::from_hms_nano_offset(13, 37, 11, 123_456_789, 3600).is_some());
    /// assert!(Time::from_hms_nano_offset(23, 59, 59, 999_999_999, -3600).is_some());
    /// assert!(Time::from_hms_nano_offset(24, 0, 0, 0, 0).is_none());
    /// assert!(Time::from_hms_nano_offset(0, 60, 0, 0, 0).is_none());
    /// assert!(Time::from_hms_nano_offset(0, 0, 60, 0, 0).is_none());
    /// assert!(Time::from_hms_nano_offset(0, 0, 59, 1_000_000_000, 0).is_none());
    /// assert!(Time::from_hms_nano_offset(0, 0, 0, 1_000_000_000, 0).is_none());
    /// ```
    pub fn from_hms_nano_offset(
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
        utc_offset: i32,
    ) -> Option<Self> {
        let time = LocalTime::from_hms_nano(hour, minute, second, nanosecond)?;
        Some(Self { time, utc_offset })
    }

    /// Return the `Time` as (hour, minute, second, nanosecond, utc_offset) tuple,
    /// where the `utc_offset` is in seconds.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Time;
    ///
    /// let time = Time::from_nanos_since_midnight(
    ///     // Hour 1
    ///     1 * 60 * 60 * 1_000_000_000
    ///     // Minute 2
    ///     + 2 * 60 * 1_000_000_000
    ///     // Second 3
    ///     + 3 * 1_000_000_000
    ///     // Nanosecond 4
    ///     + 4,
    ///     // UTC+02:00
    ///     7200,
    /// )
    /// .unwrap();
    /// assert_eq!(time.hms_nano_offset(), (1, 2, 3, 4, 7200));
    /// ```
    pub fn hms_nano_offset(&self) -> (u8, u8, u8, u32, i32) {
        let (hour, min, sec, nano) = self.time.hms_nano();
        (hour, min, sec, nano, self.utc_offset)
    }
}

#[cfg(feature = "chrono_0_4")]
mod chrono_0_4_impl {
    use super::*;

    use chrono_0_4 as chrono;

    impl Time {
        /// Convert a [`chrono::NaiveTime`] (from `chrono` version 0.4)
        /// and a [`chrono::FixedOffset`] (from `chrono` version 0.4)
        /// into a `Time`.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::NaiveTime`] or the [`chrono::FixedOffset`]
        ///   is out of range for the `Time` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveTime;
        /// use neo4j::value::time::Time;
        ///
        /// let chrono_time = NaiveTime::from_hms_nano_opt(1, 2, 3, 4).unwrap();
        /// let chrono_tz = chrono::FixedOffset::east_opt(3600).unwrap();
        /// let time = Time::from_chrono_0_4(chrono_time, chrono_tz).unwrap();
        /// assert_eq!(time.hms_nano_offset(), (1, 2, 3, 4, 3600));
        /// ```
        pub fn from_chrono_0_4(time: chrono::NaiveTime, tz: chrono::FixedOffset) -> Option<Self> {
            let time = LocalTime::from_chrono_0_4(time)?;
            let tz_offset = tz.local_minus_utc();
            Some(Self {
                time,
                utc_offset: tz_offset,
            })
        }

        /// Convert the `Time` into a ([`chrono::NaiveTime`], [`chrono::FixedOffset`]) tuple
        /// using `chrono` crate version 0.4.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `Time` is out of range for the [`chrono`] crate version 0.4.  
        ///   The exact range valid is defined by the `chrono` crate.
        /// - The `Time`'s UTC offset is out of range for the [`chrono`] crate version 0.4.  
        ///   The exact range valid is defined by the `chrono` crate.
        ///
        ///  # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveTime;
        /// use neo4j::value::time::Time;
        ///
        /// let time = Time::from_hms_nano_offset(4, 3, 2, 1, -7200).unwrap();
        /// let (chrono_time, chrono_tz) = time.to_chrono_0_4().unwrap();
        /// assert_eq!(
        ///     chrono_time,
        ///     NaiveTime::from_hms_nano_opt(4, 3, 2, 1).unwrap()
        /// );
        /// assert_eq!(chrono_tz.local_minus_utc(), -7200);
        /// ```
        pub fn to_chrono_0_4(&self) -> Option<(chrono::NaiveTime, chrono::FixedOffset)> {
            let chrono_time = self.time.to_chrono_0_4()?;
            let tz = chrono::FixedOffset::east_opt(self.utc_offset)?;
            Some((chrono_time, tz))
        }
    }
}
