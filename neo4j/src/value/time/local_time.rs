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

use super::components::TimeComponents;

/// Represents a time value (hour, minute, second, nanosecond) without time zone information in the
/// DBMS.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalTime {
    nanos: i64,
}

impl LocalTime {
    fn new_unchecked(nanos: i64) -> Self {
        debug_assert!(0 <= nanos);
        debug_assert!(nanos < 24 * 60 * 60 * 1_000_000_000);
        Self { nanos }
    }

    /// Make a new `LocalTime` from nanoseconds since midnight.
    ///
    /// Leap seconds are not supported.
    ///
    /// # Errors
    /// Returns `None` if:
    /// - `nanoseconds` is out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::LocalTime;
    ///
    /// assert!(LocalTime::from_nanos_since_midnight(0).is_some());
    /// // 23:59:59.999999999
    /// assert!(LocalTime::from_nanos_since_midnight(24 * 60 * 60 * 1_000_000_000 - 1).is_some());
    /// assert!(LocalTime::from_nanos_since_midnight(u64::MAX).is_none());
    /// ```
    pub fn from_nanos_since_midnight(nanos: u64) -> Option<Self> {
        if nanos >= 24 * 60 * 60 * 1_000_000_000 {
            return None;
        }
        debug_assert_eq!(nanos as i64 as u64, nanos);
        let nanos = nanos as i64;
        Some(Self::new_unchecked(nanos))
    }

    /// Return the `LocalTime`'s nanoseconds since midnight.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::LocalTime;
    ///
    /// let time = LocalTime::from_nanos_since_midnight(12345678912345).unwrap();
    /// assert_eq!(time.nanos_since_midnight(), 12345678912345);
    /// ```
    pub fn nanos_since_midnight(&self) -> u64 {
        let nanos = self.nanos;
        debug_assert_eq!(nanos as u64 as i64, nanos);
        nanos as u64
    }

    pub(crate) fn nanos(&self) -> i64 {
        self.nanos
    }

    /// Make a new `LocalTime` from [`TimeComponents`].
    ///
    /// # Errors
    /// Returns `None` if:
    /// - The `components` are invalid. E.g.,
    ///   - `nanoseconds` must be less than `1_000_000_000`.
    ///     I.e., leap seconds are not supported.
    ///   - Any value is out of range (e.g., `hour` > 23).
    /// - The represented `LocalTime` is out of range for the computation or storage.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    ///
    /// ```
    /// use neo4j::value::time::{LocalTime, TimeComponents};
    ///
    /// let from_hms_nano = |hour, min, sec, nano| {
    ///     let components = TimeComponents::from_hms_nano(hour, min, sec, nano);
    ///     LocalTime::from_components(components)
    /// };
    ///
    /// assert!(from_hms_nano(0, 0, 0, 0).is_some());
    /// assert!(from_hms_nano(13, 37, 11, 123_456_789).is_some());
    /// assert!(from_hms_nano(23, 59, 59, 999_999_999).is_some());
    /// assert!(from_hms_nano(24, 0, 0, 0).is_none());
    /// assert!(from_hms_nano(0, 60, 0, 0).is_none());
    /// assert!(from_hms_nano(0, 0, 60, 0).is_none());
    /// assert!(from_hms_nano(0, 0, 59, 1_000_000_000).is_none());
    /// assert!(from_hms_nano(0, 0, 0, 1_000_000_000).is_none());
    /// ```
    pub fn from_components(components: TimeComponents) -> Option<Self> {
        components
            .to_nanos_since_midnight()
            .map(Self::new_unchecked)
    }

    /// Return the `LocalTime` as [`TimeComponents`].
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{LocalTime, TimeComponents};
    ///
    /// let time = LocalTime::from_nanos_since_midnight(
    ///     // Hour 1
    ///     1 * 60 * 60 * 1_000_000_000
    ///     // Minute 2
    ///     + 2 * 60 * 1_000_000_000
    ///     // Second 3
    ///     + 3 * 1_000_000_000
    ///     // Nanosecond 4
    ///     + 4,
    /// )
    /// .unwrap();
    /// let components = time.to_components();
    /// assert_eq!(components, TimeComponents::from_hms_nano(1, 2, 3, 4));
    /// ```
    pub fn to_components(&self) -> TimeComponents {
        TimeComponents::from_nanos_since_midnight(self.nanos)
    }
}

#[cfg(feature = "chrono_0_4")]
mod chrono_0_4_impl {
    use super::super::ChronoConversionError;
    use super::*;

    use chrono::Timelike;
    use chrono_0_4 as chrono;

    impl LocalTime {
        /// Convert a [`chrono::NaiveTime`] (from `chrono` version 0.4) into a `LocalTime`.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::NaiveTime`] is out of range for the `LocalTime` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveTime;
        /// use neo4j::value::time::{LocalTime, TimeComponents};
        ///
        /// let chrono_time = NaiveTime::from_hms_nano_opt(1, 2, 3, 4).unwrap();
        /// let time = LocalTime::from_chrono_0_4(chrono_time).unwrap();
        /// assert_eq!(
        ///     time.to_components(),
        ///     TimeComponents::from_hms_nano(1, 2, 3, 4)
        /// );
        /// ```
        pub fn from_chrono_0_4(time: chrono::NaiveTime) -> Option<Self> {
            let secs: u32 = time.num_seconds_from_midnight();
            let nanos: u32 = time.nanosecond();
            if nanos >= 1_000_000_000 {
                // Leap seconds are not supported
                return None;
            }
            let nano = u64::from(secs) * 1_000_000_000 + u64::from(nanos);
            Self::from_nanos_since_midnight(nano)
        }

        /// Convert the `LocalTime` into a [`chrono::NaiveTime`] using `chrono` crate version 0.4.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `LocalTime` is out of range for the [`chrono`] crate version 0.4.  
        ///   The exact range valid is defined by the `chrono` crate.
        ///
        ///  # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveTime;
        /// use neo4j::value::time::{LocalTime, TimeComponents};
        ///
        /// let time = TimeComponents::from_hms_nano(1, 2, 3, 4);
        /// let time = LocalTime::from_components(time).unwrap();
        /// let chrono_time = time.to_chrono_0_4().unwrap();
        /// assert_eq!(
        ///     chrono_time,
        ///     NaiveTime::from_hms_nano_opt(1, 2, 3, 4).unwrap()
        /// );
        /// ```
        pub fn to_chrono_0_4(self) -> Option<chrono::NaiveTime> {
            let Self { nanos } = self;
            assert!(nanos >= 0);
            let secs = nanos / 1_000_000_000;
            let nanos: i64 = nanos % 1_000_000_000;
            debug_assert_eq!(secs as u32 as i64, secs);
            let secs = secs as u32;
            debug_assert_eq!(nanos as u32 as i64, nanos);
            let nanos = nanos as u32;
            chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        }
    }

    impl TryFrom<chrono::NaiveTime> for LocalTime {
        type Error = ChronoConversionError;

        /// See [`LocalTime::from_chrono_0_4`].
        fn try_from(value: chrono::NaiveTime) -> Result<Self, Self::Error> {
            LocalTime::from_chrono_0_4(value).ok_or(ChronoConversionError {
                source_type: "chrono::NaiveTime",
                target_type: "LocalTime",
            })
        }
    }

    impl TryFrom<LocalTime> for chrono::NaiveTime {
        type Error = ChronoConversionError;

        /// See [`LocalTime::to_chrono_0_4`].
        fn try_from(value: LocalTime) -> Result<Self, Self::Error> {
            value.to_chrono_0_4().ok_or(ChronoConversionError {
                source_type: "LocalTime",
                target_type: "chrono::NaiveTime",
            })
        }
    }
}
