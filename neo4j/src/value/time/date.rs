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

use chrono::Datelike;

use super::chrono;

/// Represents a calendar date (year, month, day) value in the DBMS.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Date {
    days: i64,
}

impl Date {
    pub(crate) fn from_chrono(date: chrono::NaiveDate) -> Option<Self> {
        // let UNIX_EPOC_DAYS = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce();
        const UNIX_EPOC_DAYS: i64 = 719163;
        let days: i64 = date.num_days_from_ce().into();
        let days = days.checked_sub(UNIX_EPOC_DAYS)?;
        Some(Self { days })
    }

    pub(crate) fn to_chrono(self) -> Option<chrono::NaiveDate> {
        let days_sinc_epoch = chrono::Duration::try_days(self.days)?;
        let epoch = chrono::NaiveDate::from_yo_opt(1970, 1).unwrap();
        epoch.checked_add_signed(days_sinc_epoch)
    }

    /// Makes a new `Date` from an ordinal since UNIX epoch.
    ///
    /// The ordinal is the number of days passed sinc UNIX epoch (1970-01-01) and
    /// can be negative.
    ///
    /// # Errors
    /// Returns `None` if:
    /// - `ordinal` is out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Date;
    ///
    /// let epoch = Date::from_ordinal(33).unwrap();
    /// assert_eq!(epoch.ymd(), Some((1970, 2, 3)));
    ///
    /// // 2023-12-08
    /// let first_driver_release = Date::from_ordinal(19699).unwrap();
    /// assert_eq!(first_driver_release.ymd(), Some((2023, 12, 8)));
    /// assert_eq!(first_driver_release.ordinal(), 19699);
    ///
    /// // 0001-01-01
    /// let ancient = Date::from_ordinal(-719528).unwrap();
    /// assert_eq!(ancient.ymd(), Some((0, 1, 1)));
    /// assert_eq!(ancient.ordinal(), -719528);
    /// ```
    pub fn from_ordinal(ordinal: i64) -> Option<Self> {
        let date = Date { days: ordinal };
        Some(date)
    }

    /// Return the `Date`'s ordinal since UNIX epoch.
    ///
    /// The ordinal is the number of days passed sinc UNIX epoch (1970-01-01) and
    /// can be negative.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Date;
    ///
    /// let epoch = Date::from_ymd(1970, 1, 1).unwrap();
    /// assert_eq!(epoch.ordinal(), 0);
    ///
    /// let first_driver_release = Date::from_ymd(2023, 12, 8).unwrap();
    /// assert_eq!(first_driver_release.ordinal(), 19699);
    ///
    /// let ancient = Date::from_ymd(1, 1, 1).unwrap();
    /// assert_eq!(ancient.ordinal(), -719162);
    /// ```
    pub fn ordinal(&self) -> i64 {
        self.days
    }

    // docs copied and adjusted from chrono.
    /// Makes a new `Date` from the (year, month, day) tuple.
    ///
    /// # Errors
    /// Returns `None` if:
    /// - The specified calendar date does not exist (for example 2023-04-31).
    /// - The value for `month` or `day` is invalid.
    /// - The resulting `DateTime` is out of range for the computation or storage.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    ///
    /// ```
    /// use neo4j::value::time::Date;
    ///
    /// assert!(Date::from_ymd(2015, 3, 14).is_some());
    /// assert!(Date::from_ymd(2015, 0, 14).is_none());
    /// assert!(Date::from_ymd(2015, 2, 29).is_none());
    /// assert!(Date::from_ymd(-4, 2, 29).is_some()); // 5 BCE is a leap year
    /// assert!(Date::from_ymd(i32::MAX, 1, 1).is_none());
    /// assert!(Date::from_ymd(i32::MIN, 1, 1).is_none());
    /// ```
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Option<Self> {
        let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
        Self::from_chrono(date)
    }

    /// Return the `Date` as (year, month, day) tuple.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The represented `DateTime` is out of range for the computation.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Date;
    ///
    /// let date = Date::from_ymd(2023, 12, 8).unwrap();
    /// assert_eq!(date.ymd(), Some((2023, 12, 8)));
    ///
    /// let date = Date::from_ordinal(365 * 400000).unwrap();
    /// assert_eq!(date.ymd(), None);
    /// ```
    pub fn ymd(&self) -> Option<(i32, u32, u32)> {
        let date = self.to_chrono()?;
        Some((date.year(), date.month(), date.day()))
    }
}

#[cfg(feature = "chrono_0_4")]
mod chrono_0_4_impl {
    use super::super::ChronoConversionError;
    use super::*;

    use chrono_0_4 as chrono;

    impl Date {
        /// Convert a [`chrono::NaiveDate`] (from `chrono` version 0.4) into a `Date`.
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The [`chrono::NaiveDate`] is out of range for the `Date` value.  
        ///   The exact range valid is considered an implementation detail and might change.
        ///
        /// # Example
        /// ```
        /// # use chrono_0_4 as chrono;
        /// use chrono::NaiveDate;
        /// use neo4j::value::time::Date;
        ///
        /// let chrono_date = NaiveDate::from_ymd_opt(2023, 12, 8).unwrap();
        /// let date = Date::from_chrono_0_4(chrono_date).unwrap();
        /// assert_eq!(date, Date::from_ymd(2023, 12, 8).unwrap());
        /// ```
        pub fn from_chrono_0_4(date: chrono::NaiveDate) -> Option<Self> {
            Self::from_chrono(date)
        }

        /// Convert the `Date` into a [`chrono::NaiveDate`] (from `chrono` crate version 0.4).
        ///
        /// This requires the `chrono_0_4` feature to be enabled.
        ///
        /// # Errors
        /// Returns `None` if
        /// - The represented `Date` is out of range for the [`chrono`] crate version 0.4.  
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
        pub fn to_chrono_0_4(&self) -> Option<chrono::NaiveDate> {
            self.to_chrono()
        }
    }

    impl TryFrom<chrono::NaiveDate> for Date {
        type Error = ChronoConversionError;

        /// See [`Date::from_chrono_0_4`].
        fn try_from(value: chrono::NaiveDate) -> Result<Self, Self::Error> {
            Date::from_chrono_0_4(value).ok_or(ChronoConversionError {
                source_type: "chrono::NaiveDate",
                target_type: "Date",
            })
        }
    }

    impl TryFrom<Date> for chrono::NaiveDate {
        type Error = ChronoConversionError;

        /// See [`Date::to_chrono_0_4`].
        fn try_from(value: Date) -> Result<Self, Self::Error> {
            value.to_chrono_0_4().ok_or(ChronoConversionError {
                source_type: "Date",
                target_type: "chrono::NaiveDate",
            })
        }
    }
}
