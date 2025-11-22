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
use super::super::Date;

use super::{DateTimeComponents, TimeComponents};

/// Helper struct to build or extract date components.
///
/// The struct does not perform any validation of the contained values.
///
/// See
///  * [`Date::from_components`], [`Date::to_components`]
///
/// # Example
/// ```
/// use neo4j::value::time::DateComponents;
///
/// let components_1 = DateComponents::from_ymd(2000, 12, 24);
/// let components_2 = DateComponents {
///     year: 2000,
///     month: 12,
///     day: 24,
/// };
/// assert_eq!(components_1, components_2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateComponents {
    pub year: i64,
    pub month: u8,
    pub day: u8,
}

impl Default for DateComponents {
    /// Create `DateComponents` for 1970-01-01 (UNIX epoch date).
    fn default() -> Self {
        Self::from_ymd(1970, 1, 1)
    }
}

pub(super) const DAYS_BEFORE_MONTH: [u16; 13] = [
    0, // enable 1-based month indexing
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, // January to December
];
pub(super) const DAYS_IN_MONTH: [u8; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
pub(super) const DAYS_IN_400_YEARS: i64 = 146097;
pub(super) const DAYS_IN_100_YEARS: i64 = 36524;
pub(super) const DAYS_IN_4_YEARS: i64 = 1461;
pub(super) const DAYS_IN_YEAR: i64 = 365;

impl DateComponents {
    pub(crate) fn from_unix_ordinal(ordinal: i64) -> Self {
        if ordinal > 0 {
            Self::from_ordinal_2001(ordinal - 11323)
        } else {
            Self::from_ordinal_1(ordinal + 719162)
        }
    }

    fn from_ordinal_2001(ordinal: i64) -> Self {
        let mut date = Self::from_ordinal_1(ordinal);
        date.year += 2000;
        date
    }

    fn from_ordinal_1(ordinal: i64) -> Self {
        let (year, ordinal) = if ordinal < 0 {
            // shift ordinal up so it's in range 0399-12-31 .. 0001-01-01
            let n = ordinal / -DAYS_IN_400_YEARS;
            let year = -400 * n + -400 + 1;
            let ordinal = ordinal - -DAYS_IN_400_YEARS * n - -DAYS_IN_400_YEARS;
            (year, ordinal)
        } else {
            (1, ordinal)
        };

        debug_assert!(ordinal >= 0);

        let year_400 = ordinal / DAYS_IN_400_YEARS;
        let ordinal = ordinal % DAYS_IN_400_YEARS;

        let year_100 = ordinal / DAYS_IN_100_YEARS;
        let ordinal = ordinal % DAYS_IN_100_YEARS;

        let year_4 = ordinal / DAYS_IN_4_YEARS;
        let ordinal = ordinal % DAYS_IN_4_YEARS;

        let year_1 = ordinal / DAYS_IN_YEAR;
        let ordinal = ordinal % DAYS_IN_YEAR;

        let year = year_400 * 400 + year_100 * 100 + year_4 * 4 + year_1 + year;
        if year_100 == 4 || year_1 == 4 {
            debug_assert_eq!(ordinal, 0);
            let year = year - 1;
            return Self {
                year,
                month: 12,
                day: 31,
            };
        }

        let leap = year_1 == 3 && (year_4 != 24 || year_100 == 3);
        debug_assert_eq!(leap, is_leap(year));

        debug_assert!(ordinal >= 0);
        debug_assert!(ordinal.abs() <= 365 + i64::from(leap));
        debug_assert_eq!(ordinal as u16 as i64, ordinal);
        let ordinal = ordinal as u16;

        let (month, day) = Self::ordinal_to_month_and_day(ordinal, leap);
        Self { year, month, day }
    }

    fn ordinal_to_month_and_day(ordinal: u16, leap: bool) -> (u8, u8) {
        debug_assert!(ordinal <= if leap { 366 } else { 365 });
        // get upper bound of month
        let mut month = (ordinal + 50) / 32;
        debug_assert!(month >= 1);
        debug_assert!(month <= 13);
        let mut preceding = DAYS_BEFORE_MONTH[month as usize] + u16::from(month > 2 && leap);
        if preceding > ordinal {
            // estimate too high
            month -= 1;
            preceding = DAYS_BEFORE_MONTH[month as usize] + u16::from(month > 2 && leap);
        }
        debug_assert_eq!(month as u8 as u16, month);
        let month = month as u8;
        debug_assert!(preceding <= ordinal);
        let day = ordinal + 1 - preceding;
        debug_assert_eq!(day as u8 as u16, day);
        let day = day as u8;
        debug_assert!(day <= DAYS_IN_MONTH[month as usize] + u8::from(month == 2 && leap));

        (month, day)
    }

    pub(crate) fn to_unix_ordinal(self) -> Option<i64> {
        if self.year < 0 {
            let ordinal = self.to_ordinal_1()?;
            ordinal.checked_sub(719162)
        } else {
            let ordinal = self.to_ordinal_2001()?;
            ordinal.checked_add(11323)
        }
    }

    fn to_ordinal_2001(mut self) -> Option<i64> {
        self.year -= 2000;
        self.to_ordinal_1()
    }

    fn to_ordinal_1(mut self) -> Option<i64> {
        let (ordinal_1, ordinal_2) = if self.year > 0 {
            self.year -= 1;
            (0, 0)
        } else {
            // shift year up so it's in range 400 .. 1
            let n = self.year / -400;
            // self.year -= -400 * n + -400 + 1;
            self.year = self.year - -400 * n + 400 - 1;
            (DAYS_IN_400_YEARS.checked_mul(-n)?, -DAYS_IN_400_YEARS)
        };

        debug_assert!(self.year >= 0);

        let year_400 = self.year / 400;
        let year_400_mod = self.year % 400;

        let year_100 = year_400_mod / 100;
        let year_100_mod = year_400_mod % 100;

        let year_4 = year_100_mod / 4;
        let year_4_mod = year_100_mod % 4;

        let leap = year_400_mod == 399 || (year_4_mod == 3 && year_100_mod != 99);
        debug_assert_eq!(leap, is_leap(self.year + 1));

        let year_ordinal = i64::from(Self::month_and_day_to_ordinal(self.month, self.day, leap)?);

        let ordinal = year_ordinal
            .checked_add(year_400.checked_mul(DAYS_IN_400_YEARS)?)?
            .checked_add(year_100.checked_mul(DAYS_IN_100_YEARS)?)?
            .checked_add(year_4.checked_mul(DAYS_IN_4_YEARS)?)?
            .checked_add(year_4_mod.checked_mul(DAYS_IN_YEAR)?)?
            .checked_add(ordinal_1)?
            .checked_add(ordinal_2)?;

        Some(ordinal)
    }

    fn month_and_day_to_ordinal(month: u8, day: u8, leap: bool) -> Option<u16> {
        if !(1..=12).contains(&month) {
            return None;
        }
        let days_in_month = DAYS_IN_MONTH[month as usize] + u8::from(month == 2 && leap);
        if !(1..=days_in_month).contains(&day) {
            return None;
        }

        let ordinal =
            DAYS_BEFORE_MONTH[month as usize] + u16::from(month > 2 && leap) + u16::from(day - 1);
        Some(ordinal)
    }

    /// Create `DateComponents` for 1970-01-01 (UNIX epoch date).
    pub fn new() -> Self {
        Self::default()
    }

    /// Return `DateComponents` as (year, month, and day) tuple.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateComponents;
    ///
    /// let components = DateComponents::from_ymd(2023, 12, 8);
    /// let (year, month, day) = components.ymd();
    ///
    /// assert_eq!(year, 2023);
    /// assert_eq!(month, 12);
    /// assert_eq!(day, 8);
    /// ```
    pub fn ymd(&self) -> (i64, u8, u8) {
        let Self { year, month, day } = *self;
        (year, month, day)
    }

    /// Create `DateComponents` from the given year, month, and day.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateComponents;
    ///
    /// let components = DateComponents::from_ymd(2023, 12, 8);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// ```
    pub fn from_ymd(year: i64, month: u8, day: u8) -> Self {
        Self { year, month, day }
    }

    /// Set the year, month, and day of the `DateComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::DateComponents;
    ///
    /// let components = DateComponents::new().with_ymd(2023, 12, 8);
    ///
    /// assert_eq!(components.year, 2023);
    /// assert_eq!(components.month, 12);
    /// assert_eq!(components.day, 8);
    /// ```
    pub fn with_ymd(mut self, year: i64, month: u8, day: u8) -> Self {
        self.year = year;
        self.month = month;
        self.day = day;
        self
    }

    /// Set the hour, minute, and second
    /// turning the `DateComponents` into `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents};
    ///
    /// let components: DateTimeComponents = DateComponents::new().with_hms(1, 2, 3);
    ///
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 0);
    /// ```
    pub fn with_hms(self, hour: u8, min: u8, sec: u8) -> DateTimeComponents {
        self.with_hms_nano(hour, min, sec, 0)
    }

    /// Set the hour, minute, second, and nanosecond
    /// turning the `DateComponents` into `DateTimeComponents`.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::{DateComponents, DateTimeComponents};
    ///
    /// let components: DateTimeComponents = DateComponents::new().with_hms_nano(1, 2, 3, 4);
    ///
    /// assert_eq!(components.hour, 1);
    /// assert_eq!(components.min, 2);
    /// assert_eq!(components.sec, 3);
    /// assert_eq!(components.nano, 4);
    /// ```
    pub fn with_hms_nano(self, hour: u8, min: u8, sec: u8, nano: u32) -> DateTimeComponents {
        let time = TimeComponents::from_hms_nano(hour, min, sec, nano);
        DateTimeComponents::combine(self, time)
    }
}

const fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Datelike;
    use chrono_0_4 as chrono;
    use rstest::rstest;

    #[test]
    fn test_days_in_400_years() {
        let mut days: i64 = 0;
        for year in 0..400 {
            days += DAYS_IN_MONTH.into_iter().map(i64::from).sum::<i64>();
            days += i64::from(is_leap(year));
        }
        assert_eq!(days, DAYS_IN_400_YEARS);
    }

    #[test]
    fn test_days_in_100_years() {
        let mut days: i64 = 0;
        for year in 100..200 {
            days += DAYS_IN_MONTH.into_iter().map(i64::from).sum::<i64>();
            days += i64::from(is_leap(year));
        }
        assert_eq!(days, DAYS_IN_100_YEARS);
    }

    #[test]
    fn test_days_in_4_years() {
        let mut days: i64 = 0;
        for year in 0..4 {
            days += DAYS_IN_MONTH.into_iter().map(i64::from).sum::<i64>();
            days += i64::from(is_leap(year));
        }
        assert_eq!(days, DAYS_IN_4_YEARS);
    }

    #[test]
    fn test_days_in_year() {
        let days: i64 = DAYS_IN_MONTH.iter().map(|&d| i64::from(d)).sum();
        assert_eq!(days, DAYS_IN_YEAR);
    }

    #[rstest]
    #[case(i64::MIN, -25252734927764585, 6, 7)]
    #[case(-719528, 0, 1, 1)]
    #[case(-719162, 1, 1, 1)]
    #[case(0, 1970, 1, 1)]
    #[case(20058, 2024, 12, 1)]
    #[case(i64::MAX, 25_252_734_927_768_524, 7, 27)]
    fn test_from_ordinal(
        #[case] ordinal: i64,
        #[case] expected_year: i64,
        #[case] expected_month: u8,
        #[case] expected_day: u8,
    ) {
        let DateComponents { year, month, day } = DateComponents::from_unix_ordinal(ordinal);

        // dbg!(ordinal, year, month, day);

        assert_eq!(year, expected_year);
        assert_eq!(month, expected_month);
        assert_eq!(day, expected_day);
    }

    #[rstest]
    #[case(None, -25252734927764586, 6, 7)]
    #[case(None, -25252734927764585, 5, 7)]
    #[case(None, -25252734927764585, 6, 6)]
    #[case(Some(i64::MIN), -25252734927764585, 6, 7)]
    #[case(Some(-719528), 0, 1, 1)]
    #[case(Some(-719162), 1, 1, 1)]
    #[case(Some(0), 1970, 1, 1)]
    #[case(Some(12), 1970, 1, 13)]
    #[case(Some(31), 1970, 2, 1)]
    #[case(Some(193), 1970, 7, 13)]
    #[case(Some(11323), 2001, 1, 1)]
    #[case(Some(19723), 2024, 1, 1)]
    #[case(Some(20058), 2024, 12, 1)]
    #[case(Some(i64::MAX), 25_252_734_927_768_524, 7, 27)]
    #[case(None, 25_252_734_927_768_524, 7, 28)]
    #[case(None, 25_252_734_927_768_524, 8, 27)]
    #[case(None, 25_252_734_927_768_525, 7, 27)]
    fn test_to_ordinal(
        #[case] expected_ordinal: Option<i64>,
        #[case] year: i64,
        #[case] month: u8,
        #[case] day: u8,
    ) {
        let date = DateComponents { year, month, day };
        let ordinal = date.to_unix_ordinal();

        assert_eq!(ordinal, expected_ordinal);

        let Some(ordinal) = ordinal else {
            return;
        };
        let date = DateComponents::from_unix_ordinal(ordinal);
        assert_eq!(date.year, year);
        assert_eq!(date.month, month);
        assert_eq!(date.day, day);
    }

    #[rstest]
    #[case(-4, true, -1827)]
    #[case(-3, false, -1461)]
    #[case(-1, false, -731)]
    #[case(0, true, -366)]
    #[case(1, false, 0)]
    #[case(2, false, 365)]
    #[case(4, true, 1095)]
    #[case(5, false, 1461)]
    #[case(8, true, 2556)]
    #[case(50, false, 17897)]
    #[case(99, false, 35794)]
    #[case(100, false, 36159)]
    #[case(101, false, 36524)]
    #[case(120, true, 43463)]
    #[case(150, false, 54421)]
    #[case(199, false, 72318)]
    #[case(200, false, 72683)]
    #[case(201, false, 73048)]
    #[case(216, true, 78526)]
    #[case(396, true, 144270)]
    #[case(399, false, 145366)]
    #[case(400, true, 145731)]
    #[case(401, false, 146097)]
    fn test_from_ordinal_1(
        #[case] expected_year: i64,
        #[case] leap: bool,
        #[case] year_offset: i64,
    ) {
        let leap_int = i64::from(leap);
        for (expected_month, expected_day, ordinal) in [
            (1, 1, year_offset),
            (1, 15, year_offset + 14),
            (1, 31, year_offset + 30),
            (2, 1, year_offset + 31),
            (2, 15, year_offset + 31 + 14),
            if leap {
                (2, 29, year_offset + 31 + 28)
            } else {
                (2, 28, year_offset + 31 + 27)
            },
            // 59 + leap_int = 31 + (28 + leap_int)
            (3, 1, year_offset + 59 + leap_int),
            (3, 15, year_offset + 59 + leap_int + 14),
            (3, 31, year_offset + 59 + leap_int + 30),
            // 90 + leap_int = 31 + (28 + leap_int) + 31
            (4, 1, year_offset + 90 + leap_int),
            (4, 15, year_offset + 90 + leap_int + 14),
            (4, 30, year_offset + 90 + leap_int + 29),
            // 120 + leap_int = 31 + (28 + leap_int) + 31 + 30
            (5, 1, year_offset + 120 + leap_int),
            (5, 15, year_offset + 120 + leap_int + 14),
            (5, 31, year_offset + 120 + leap_int + 30),
            // 151 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31
            (6, 1, year_offset + 151 + leap_int),
            (6, 15, year_offset + 151 + leap_int + 14),
            (6, 30, year_offset + 151 + leap_int + 29),
            // 181 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31 + 30
            (7, 1, year_offset + 181 + leap_int),
            (7, 15, year_offset + 181 + leap_int + 14),
            (7, 31, year_offset + 181 + leap_int + 30),
            // 212 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31 + 30 + 31
            (8, 1, year_offset + 212 + leap_int),
            (8, 15, year_offset + 212 + leap_int + 14),
            (8, 31, year_offset + 212 + leap_int + 30),
            // 243 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31 + 30 + 31 + 31
            (9, 1, year_offset + 243 + leap_int),
            (9, 15, year_offset + 243 + leap_int + 14),
            (9, 30, year_offset + 243 + leap_int + 29),
            // 273 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31 + 30 + 31 + 31 + 30
            (10, 1, year_offset + 273 + leap_int),
            (10, 15, year_offset + 273 + leap_int + 14),
            (10, 31, year_offset + 273 + leap_int + 30),
            // 304 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31
            (11, 1, year_offset + 304 + leap_int),
            (11, 15, year_offset + 304 + leap_int + 14),
            (11, 30, year_offset + 304 + leap_int + 29),
            // 334 + leap_int = 31 + (28 + leap_int) + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30
            (12, 1, year_offset + 334 + leap_int),
            (12, 15, year_offset + 334 + leap_int + 14),
            (12, 31, year_offset + 334 + leap_int + 30),
        ] {
            let DateComponents { year, month, day } = DateComponents::from_ordinal_1(ordinal);

            // dbg!(ordinal, year, month, day);

            assert_eq!(year, expected_year);
            assert_eq!(month, expected_month);
            assert_eq!(day, expected_day);
        }
    }

    #[test]
    fn test_from_ordinal_1_behaves_like_chrono() {
        for ordinal in -5_000_000..=5_000_000 {
            let date = DateComponents::from_ordinal_1(ordinal);
            let chrono_epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let chrono_delta = chrono::Duration::days(ordinal);
            let chrono_date = chrono_epoch + chrono_delta;

            // dbg!(ordinal, date, chrono_date);

            assert_eq!(i64::from(chrono_date.year()), date.year);
            assert_eq!(chrono_date.month(), u32::from(date.month));
            assert_eq!(chrono_date.day(), u32::from(date.day));

            assert_eq!(date.to_ordinal_1(), Some(ordinal));
        }
    }

    #[test]
    fn test_from_ordinal_1_limit_behaves_like_chrono() {
        for ordinal in (i64::MIN..=i64::MIN + 300_000).chain(i64::MAX - 300_000..=i64::MAX) {
            let date = DateComponents::from_ordinal_1(ordinal);
            let chrono_ordinal = ordinal % DAYS_IN_400_YEARS;
            let chrono_extra_years = ordinal / DAYS_IN_400_YEARS * 400;
            let chrono_epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
            let chrono_delta = chrono::Duration::days(chrono_ordinal);
            let chrono_date = chrono_epoch + chrono_delta;

            // dbg!(ordinal, date, chrono_date);

            assert_eq!(
                i64::from(chrono_date.year()) + chrono_extra_years,
                date.year
            );
            assert_eq!(chrono_date.month(), u32::from(date.month));
            assert_eq!(chrono_date.day(), u32::from(date.day));

            assert_eq!(date.to_ordinal_1(), Some(ordinal));
        }
    }
}
