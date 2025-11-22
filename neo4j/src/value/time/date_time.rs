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

use std::str::FromStr;

use chrono::TimeZone;

use super::{chrono, chrono_tz, local_date_time_from_timestamp, DateTimeComponents};

/// Represents a date (year, month, day) and time (hour, minute, second, nanoseconds) within a named
/// time zone in the DBMS.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DateTime {
    secs: i64,
    nanos: u32,
    tz_name: String,
}

impl DateTime {
    pub(crate) fn from_chrono(date_time: &chrono::DateTime<chrono_tz::Tz>) -> Option<Self> {
        let sec = date_time.timestamp();
        let nano = date_time.timestamp_subsec_nanos();
        if nano >= 1_000_000_000 {
            // Leap seconds are not supported
            return None;
        }
        let tz_name = date_time.timezone().name();
        Self::from_utc_timestamp(sec, nano, tz_name)
    }

    pub(crate) fn to_chrono(&self) -> Option<chrono::DateTime<chrono_tz::Tz>> {
        let Self {
            secs,
            nanos,
            tz_name,
        } = self;
        let tz = chrono_tz::Tz::from_str(tz_name).ok()?;
        let dt = local_date_time_from_timestamp(*secs, *nanos)?;
        Some(tz.from_utc_datetime(&dt))
    }

    /// Make a new `DateTime` from the given timestamp in non-leap seconds and nanoseconds
    /// elapsed since UNIX epoch in UTC (1970-01-01T00:00:00Z) and a time zone name.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The value for `nanoseconds` is invalid (must be less than `1_000_000_000`).
    /// - `seconds` is out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTime;
    ///
    /// // Create DateTime at 1970-01-01T00:00:00Z in Etc/GMT-2 timezone.
    /// // i.e., 1970-01-01T02:00:00+02:00 in local time.
    /// let dt = DateTime::from_utc_timestamp(0, 0, "Etc/GMT-2").unwrap();
    /// assert_eq!(dt.utc_timestamp(), (0, 0));
    ///
    /// let dt = DateTime::from_utc_timestamp(2, 1_000_000_001, "UTC");
    /// assert!(dt.is_none());
    /// ```
    pub fn from_utc_timestamp<S: Into<String>>(secs: i64, nanos: u32, tz_name: S) -> Option<Self> {
        if nanos >= 1_000_000_000 {
            return None;
        }
        let tz_name = tz_name.into();
        Some(Self {
            secs,
            nanos,
            tz_name,
        })
    }

    /// Return the timestamp of this `DateTime` as non-leap seconds and nanoseconds elapsed
    /// since UNIX epoch in UTC (1970-01-01T00:00:00Z).
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTime, DateTimeComponents};
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 1, 1).with_hms_nano(0, 0, 12, 34);
    /// let dt = DateTime::from_utc_components(components, "NZST").unwrap();
    /// assert_eq!(dt.utc_timestamp(), (12, 34));
    /// ```
    pub fn utc_timestamp(&self) -> (i64, u32) {
        (self.secs, self.nanos)
    }

    /// Return the time zone name of this `DateTime`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateTime;
    ///
    /// let dt = DateTime::from_utc_timestamp(0, 0, "Ether/Neverland").unwrap();
    /// assert_eq!(dt.timezone_name(), "Ether/Neverland");
    /// ```
    pub fn timezone_name(&self) -> &str {
        &self.tz_name
    }

    /// Make a new `DateTime` from [`DateTimeComponents`] in UTC along with the time zone name.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The `components` are invalid. E.g.,
    ///   - `nanoseconds` must be less than `1_000_000_000`.
    ///     I.e., leap seconds are not supported.
    ///   - `components` represents a non-existent date like 2023-04-31.
    ///   - Any value is out of range (e.g., `month` > 12).
    /// - The represented `DateTime` is out of range for the computation or storage.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTime, DateTimeComponents};
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 1, 1);
    /// let dt = DateTime::from_utc_components(components, "UTC").unwrap();
    /// assert_eq!(dt.utc_timestamp(), (0, 0));
    /// assert_eq!(dt.timezone_name(), "UTC");
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 2, 32);
    /// assert!(DateTime::from_utc_components(components, "UTC").is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 13, 1);
    /// assert!(DateTime::from_utc_components(components, "UTC").is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(i64::MAX, 1, 1);
    /// assert!(DateTime::from_utc_components(components, "UTC").is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(i64::MIN, 1, 1);
    /// assert!(DateTime::from_utc_components(components, "UTC").is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(1970, 0, 1);
    /// assert!(DateTime::from_utc_components(components, "UTC").is_none());
    ///
    /// let components = DateTimeComponents::from_ymd(-3, 2, 29); // 4 BCE is not a leap year
    /// assert!(DateTime::from_utc_components(components, "UTC").is_none());
    /// ```
    pub fn from_utc_components<S: Into<String>>(
        components: DateTimeComponents,
        tz_name: S,
    ) -> Option<Self> {
        fn inner(components: DateTimeComponents, tz_name: String) -> Option<DateTime> {
            let (secs, nanos) = components.to_unix_timestamp()?;
            Some(DateTime {
                secs,
                nanos,
                tz_name,
            })
        }

        inner(components, tz_name.into())
    }

    /// Return the `DateTime` as [`DateTimeComponents`] in UTC along with the time zone name.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The represented `DateTime` is out of range for the computation.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateTime, DateTimeComponents};
    ///
    /// let dt = DateTime::from_utc_timestamp(0, 0, "UTC").unwrap();
    /// let (components, tz_name) = dt.to_utc_components().unwrap();
    /// assert_eq!(tz_name, "UTC");
    /// assert_eq!(components, DateTimeComponents::from_ymd(1970, 1, 1))
    /// ```
    pub fn to_utc_components(&self) -> Option<(DateTimeComponents, &str)> {
        let Self {
            secs,
            nanos,
            ref tz_name,
        } = *self;
        let components = DateTimeComponents::from_unix_timestamp(secs, nanos);
        Some((components, tz_name))
    }
}

#[cfg(all(feature = "chrono_0_4", feature = "chrono-tz_0_9"))]
mod chrono_0_4_chrono_tz_0_9_impl {
    use super::super::ChronoConversionError;
    use super::*;

    use chrono_0_4 as chrono;
    use chrono_tz_0_9 as chrono_tz;

    impl DateTime {
        /// Convert a [`chrono::DateTime`] (from `chrono` version 0.4)
        /// with a [`chrono_tz::Tz`] (from `chrono-tz` version 0.9)
        /// into a `DateTime`.
        ///
        /// This requires the `chrono_0_4` and `chrono-tz_0_9` features to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::DateTime`] is out of range for the `DateTime` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        /// - The [`chrono::DateTime`] represents a leap second.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// # use chrono_tz_0_9 as chrono_tz;
        /// use chrono::{TimeZone, Timelike};
        /// use chrono_tz::Tz;
        /// use neo4j::value::time::DateTime;
        ///
        /// let tz: Tz = "Etc/GMT+1".parse().unwrap();
        /// let chrono_dt = tz
        ///     .with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
        ///     .single()
        ///     .unwrap()
        ///     .with_nanosecond(123)
        ///     .unwrap();
        /// let dt = DateTime::from_chrono_0_4_chrono_tz_0_9(&chrono_dt).unwrap();
        /// let (sec, nano) = dt.utc_timestamp();
        /// assert_eq!(sec, 3600);
        /// assert_eq!(nano, 123);
        /// assert_eq!(dt.timezone_name(), "Etc/GMT+1");
        ///
        /// let chrono_dt = tz
        ///     .with_ymd_and_hms(1970, 1, 1, 0, 0, 59)
        ///     .single()
        ///     .unwrap()
        ///     .with_nanosecond(1_000_000_001)
        ///     .unwrap();
        /// let dt = DateTime::from_chrono_0_4_chrono_tz_0_9(&chrono_dt);
        /// assert!(dt.is_none()); // Leap seconds not supported
        /// ```
        pub fn from_chrono_0_4_chrono_tz_0_9(
            date_time: &chrono::DateTime<chrono_tz::Tz>,
        ) -> Option<Self> {
            let sec = date_time.timestamp();
            let nano = date_time.timestamp_subsec_nanos();
            let tz_name = date_time.timezone().name();
            Self::from_utc_timestamp(sec, nano, tz_name)
        }

        /// Convert the `DateTime` into a [`chrono::DateTime`]
        /// with a [`chrono_tz::Tz`] using `chrono` 0.4 and `chrono-tz` 0.9.
        ///
        /// This requires the `chrono_0_4` and `chrono-tz_0_9` features to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `DateTime` is out of range for the [`chrono`] crate
        ///   version 0.4 or the [`chrono-tz`](`chrono_tz`) crate version 0.9.
        ///   The exact range valid is defined by the `chrono` and `chrono-tz` crates.
        /// - The [`chrono_tz::Tz`] does not know `DateTime` time zone.
        ///
        ///  # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// # use chrono_tz_0_9 as chrono_tz;
        /// use chrono::{Datelike, Timelike};
        /// use neo4j::value::time::{DateTime, DateTimeComponents};
        ///
        /// let components =
        ///     DateTimeComponents::from_ymd(1946, 7, 1).with_hms_nano(23, 59, 59, 999_999_999);
        /// let date_time = DateTime::from_utc_components(components, "Pacific/Majuro").unwrap();
        ///
        /// let chrono_date_time = date_time.to_chrono_0_4_chrono_tz_0_9().unwrap();
        /// assert_eq!(chrono_date_time.timezone(), chrono_tz::Pacific::Majuro);
        /// assert_eq!(chrono_date_time.to_utc().year(), 1946);
        /// assert_eq!(chrono_date_time.to_utc().month(), 7);
        /// assert_eq!(chrono_date_time.to_utc().day(), 1);
        /// assert_eq!(chrono_date_time.to_utc().hour(), 23);
        /// assert_eq!(chrono_date_time.to_utc().minute(), 59);
        /// assert_eq!(chrono_date_time.to_utc().second(), 59);
        /// assert_eq!(chrono_date_time.to_utc().nanosecond(), 999_999_999);
        /// ```
        pub fn to_chrono_0_4_chrono_tz_0_9(&self) -> Option<chrono::DateTime<chrono_tz::Tz>> {
            let Self {
                secs: sec,
                nanos: nano,
                tz_name,
            } = self;
            let tz = chrono_tz::Tz::from_str(tz_name).ok()?;
            let dt = local_date_time_from_timestamp(*sec, *nano)?;
            Some(tz.from_utc_datetime(&dt))
        }
    }

    impl TryFrom<&chrono::DateTime<chrono_tz::Tz>> for DateTime {
        type Error = ChronoConversionError;

        /// See [`DateTime::from_chrono_0_4_chrono_tz_0_9`].
        fn try_from(value: &chrono::DateTime<chrono_tz::Tz>) -> Result<Self, Self::Error> {
            DateTime::from_chrono_0_4_chrono_tz_0_9(value).ok_or(ChronoConversionError {
                source_type: "chrono::DateTime<chrono_tz::Tz>",
                target_type: "DateTime",
            })
        }
    }

    impl TryFrom<&DateTime> for chrono::DateTime<chrono_tz::Tz> {
        type Error = ChronoConversionError;

        /// See [`DateTime::to_chrono_0_4_chrono_tz_0_9`].
        fn try_from(value: &DateTime) -> Result<Self, Self::Error> {
            value
                .to_chrono_0_4_chrono_tz_0_9()
                .ok_or(ChronoConversionError {
                    source_type: "DateTime",
                    target_type: "chrono::DateTime<chrono_tz::Tz>",
                })
        }
    }
}

#[cfg(all(feature = "chrono_0_4", feature = "chrono-tz_0_10"))]
mod chrono_0_4_chrono_tz_0_10_impl {
    use super::super::ChronoConversionError;
    use super::*;

    use chrono_0_4 as chrono;
    use chrono_tz_0_10 as chrono_tz;

    impl DateTime {
        /// Convert a [`chrono::DateTime`] (from `chrono` version 0.4)
        /// with a [`chrono_tz::Tz`] (from `chrono-tz` version 0.10)
        /// into a `DateTime`.
        ///
        /// This requires the `chrono_0_4` and `chrono-tz_0_10` features to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::DateTime`] is out of range for the `DateTime` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        /// - The [`chrono::DateTime`] represents a leap second.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// # use chrono_tz_0_10 as chrono_tz;
        /// use chrono::{TimeZone, Timelike};
        /// use chrono_tz::Tz;
        /// use neo4j::value::time::DateTime;
        ///
        /// let tz: Tz = "Etc/GMT+1".parse().unwrap();
        /// let chrono_dt = tz
        ///     .with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
        ///     .single()
        ///     .unwrap()
        ///     .with_nanosecond(123)
        ///     .unwrap();
        /// let dt = DateTime::from_chrono_0_4_chrono_tz_0_10(&chrono_dt).unwrap();
        /// let (sec, nano) = dt.utc_timestamp();
        /// assert_eq!(sec, 3600);
        /// assert_eq!(nano, 123);
        /// assert_eq!(dt.timezone_name(), "Etc/GMT+1");
        ///
        /// let chrono_dt = tz
        ///     .with_ymd_and_hms(1970, 1, 1, 0, 0, 59)
        ///     .single()
        ///     .unwrap()
        ///     .with_nanosecond(1_000_000_001)
        ///     .unwrap();
        /// let dt = DateTime::from_chrono_0_4_chrono_tz_0_10(&chrono_dt);
        /// assert!(dt.is_none()); // Leap seconds not supported
        /// ```
        pub fn from_chrono_0_4_chrono_tz_0_10(
            date_time: &chrono::DateTime<chrono_tz::Tz>,
        ) -> Option<Self> {
            Self::from_chrono(date_time)
        }

        /// Convert the `DateTime` into a [`chrono::DateTime`]
        /// with a [`chrono_tz::Tz`] using `chrono` 0.4 and `chrono-tz` 0.10.
        ///
        /// This requires the `chrono_0_4` and `chrono-tz_0_10` features to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `DateTime` is out of range for the [`chrono`] crate
        ///   version 0.4 or the [`chrono-tz`](`chrono_tz`) crate version 0.10.  
        ///   The exact range valid is defined by the `chrono` and `chrono-tz` crates.
        /// - The [`chrono_tz::Tz`] does not know `DateTime` time zone.
        ///
        ///  # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// # use chrono_tz_0_10 as chrono_tz;
        /// use chrono::{Datelike, Timelike};
        /// use neo4j::value::time::{DateTime, DateTimeComponents};
        ///
        /// let components =
        ///     DateTimeComponents::from_ymd(1946, 7, 1).with_hms_nano(23, 59, 59, 999_999_999);
        /// let date_time = DateTime::from_utc_components(components, "Pacific/Majuro").unwrap();
        ///
        /// let chrono_date_time = date_time.to_chrono_0_4_chrono_tz_0_10().unwrap();
        /// assert_eq!(chrono_date_time.timezone(), chrono_tz::Pacific::Majuro);
        /// assert_eq!(chrono_date_time.to_utc().year(), 1946);
        /// assert_eq!(chrono_date_time.to_utc().month(), 7);
        /// assert_eq!(chrono_date_time.to_utc().day(), 1);
        /// assert_eq!(chrono_date_time.to_utc().hour(), 23);
        /// assert_eq!(chrono_date_time.to_utc().minute(), 59);
        /// assert_eq!(chrono_date_time.to_utc().second(), 59);
        /// assert_eq!(chrono_date_time.to_utc().nanosecond(), 999_999_999);
        /// ```
        pub fn to_chrono_0_4_chrono_tz_0_10(&self) -> Option<chrono::DateTime<chrono_tz::Tz>> {
            self.to_chrono()
        }
    }

    impl TryFrom<&chrono::DateTime<chrono_tz::Tz>> for DateTime {
        type Error = ChronoConversionError;

        /// See [`DateTime::from_chrono_0_4_chrono_tz_0_10`].
        fn try_from(value: &chrono::DateTime<chrono_tz::Tz>) -> Result<Self, Self::Error> {
            DateTime::from_chrono_0_4_chrono_tz_0_10(value).ok_or(ChronoConversionError {
                source_type: "chrono::DateTime<chrono_tz::Tz>",
                target_type: "DateTime",
            })
        }
    }

    impl TryFrom<&DateTime> for chrono::DateTime<chrono_tz::Tz> {
        type Error = ChronoConversionError;

        /// See [`DateTime::to_chrono_0_4_chrono_tz_0_10`].
        fn try_from(value: &DateTime) -> Result<Self, Self::Error> {
            value
                .to_chrono_0_4_chrono_tz_0_10()
                .ok_or(ChronoConversionError {
                    source_type: "DateTime",
                    target_type: "chrono::DateTime<chrono_tz::Tz>",
                })
        }
    }
}
