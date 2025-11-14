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

use duplicate::duplicate_item;

// imports for docs
#[allow(unused)]
use super::chrono;

const AVERAGE_SECONDS_IN_MONTH: i64 = 2629746;
const AVERAGE_SECONDS_IN_DAY: i64 = 86400;

/// Represents a duration in the DBMS.
///
/// Durations are composed of months, days, seconds and nanoseconds.
/// No conversion methods to/from [`chrono`]'s `Duration`/`TimeDelta` are provided as `chrono`
/// tracks only seconds and nanoseconds.
///
/// Be aware of the (limitations)[`super#limitations`] of this crate's temporal types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Duration {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) secs: i64,
    pub(crate) nanos: i32,
}

impl Duration {
    /// Make a new `Duration` from its components:
    /// - `months`
    /// - `days`
    /// - `seconds`
    /// - `nanoseconds`
    ///
    /// Excess nanoseconds are normalized into seconds.
    /// All components can have a separate sign, except for nanoseconds which always follows the
    /// sign of seconds after normalization.
    ///
    /// # Errors
    /// Returns `None` if
    /// - The components are out of range.  
    ///   The exact range valid is considered an implementation detail and might change.
    ///
    /// # Example
    /// ```
    /// use neo4j::value::time::Duration;
    ///
    /// // 1 month, -2 days, 3 seconds and 400 nanoseconds
    /// assert!(Duration::new(1, -2, 3, 400).is_some());
    /// assert!(Duration::new(i64::MAX, i64::MAX, i64::MAX, i32::MAX).is_none());
    /// assert!(Duration::new(i64::MIN, i64::MIN, i64::MIN, i32::MIN).is_none());
    ///
    /// // Nanoseconds normalized into seconds with matching sign
    /// let duration = Duration::new(0, 0, 3, -100_000_000).unwrap();
    /// assert_eq!(duration.seconds(), 2);
    /// assert_eq!(duration.nanoseconds(), 900_000_000);
    ///
    /// let duration = Duration::new(0, 0, -3, 100_000_000).unwrap();
    /// assert_eq!(duration.seconds(), -2);
    /// assert_eq!(duration.nanoseconds(), -900_000_000);
    ///
    /// let duration = Duration::new(0, 0, 3, 1_100_000_000).unwrap();
    /// assert_eq!(duration.seconds(), 4);
    /// assert_eq!(duration.nanoseconds(), 100_000_000);
    ///
    /// let duration = Duration::new(0, 0, -3, -1_100_000_000).unwrap();
    /// assert_eq!(duration.seconds(), -4);
    /// assert_eq!(duration.nanoseconds(), -100_000_000);
    ///
    /// let duration = Duration::new(0, 0, 1, -2_100_000_000).unwrap();
    /// assert_eq!(duration.seconds(), -1);
    /// assert_eq!(duration.nanoseconds(), -100_000_000);
    ///
    /// let duration = Duration::new(0, 0, -1, 2_100_000_000).unwrap();
    /// assert_eq!(duration.seconds(), 1);
    /// assert_eq!(duration.nanoseconds(), 100_000_000);
    /// ```
    pub fn new(months: i64, days: i64, secs: i64, nanos: i32) -> Option<Self> {
        let secs = secs.checked_add(i64::from(nanos) / 1_000_000_000)?;
        let nanos = nanos % 1_000_000_000;

        /*
        Arithmetic way to ensure that seconds and nanoseconds have the same sign:
         secs_sign | nanos_sign | correction
        -----------+------------+------------
                 1 |          1 |          0
                 1 |         -1 |         -1
                -1 |          1 |          1
                -1 |         -1 |          0
                 * |          0 |          0
                 0 |          * |          0

        Branch-free equivalent of:
        ```
        let (secs, nanos) = match secs {
            _ if secs > 0 && nanos < 0 => {
                (secs.checked_sub(1)?, nanos + 1_000_000_000)
            }
            _ if secs < 0 && nanos > 0 => {
                (secs.checked_add(1)?, nanos - 1_000_000_000)
            }
            _ => (secs, nanos),
        };
        ```
        */
        let secs_sign = secs.signum() as i32;
        let nanos_sign = nanos.signum();
        let different_sign: i32 =
            (secs_sign != 0 && nanos_sign != 0 && secs_sign != nanos_sign).into();
        let correction = different_sign * (secs_sign + 2 * nanos_sign);
        let (secs, nanos) = (
            secs.checked_add(correction.into())?,
            nanos - correction * 1_000_000_000,
        );

        // Range check
        let months_seconds = months.checked_mul(AVERAGE_SECONDS_IN_MONTH)?;
        let days_seconds = days.checked_mul(AVERAGE_SECONDS_IN_DAY)?;
        secs.checked_add(months_seconds)?
            .checked_add(days_seconds)?;

        Some(Self {
            months,
            days,
            secs,
            nanos,
        })
    }

    #[duplicate_item(
        name            attr       type_;
        [ months ]      [ months ] [ i64 ];
        [ days ]        [ days ]   [ i64 ];
        [ seconds ]     [ secs ]   [ i64 ];
        [ nanoseconds ] [ nanos ]  [ i32 ];
    )]
    #[doc = concat!("Get the ", stringify!(name), " component of the duration.")]
    pub fn name(&self) -> type_ {
        self.attr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case(3, -100_000_000, 2, 900_000_000)]
    #[case(-3, 100_000_000, -2, -900_000_000)]
    #[case(3, 100_000_000, 3, 100_000_000)]
    #[case(-3, -100_000_000, -3, -100_000_000)]
    #[case(1, -2_100_000_000, -1, -100_000_000)]
    #[case(-1, 2_100_000_000, 1, 100_000_000)]
    #[case(0, 0, 0, 0)]
    #[case(1, 0, 1, 0)]
    #[case(0, 1, 0, 1)]
    #[case(-1, 0, -1, 0)]
    #[case(0, -1, 0, -1)]
    fn test_duration_new_matching_s_ns_sign(
        #[case] sec_in: i64,
        #[case] nano_in: i32,
        #[case] sec_out: i64,
        #[case] nano_out: i32,
    ) {
        let duration = Duration::new(0, 0, sec_in, nano_in).unwrap();
        assert_eq!(duration.months(), 0);
        assert_eq!(duration.days(), 0);
        assert_eq!(duration.seconds(), sec_out);
        assert_eq!(duration.nanoseconds(), nano_out);
    }

    #[rstest]
    #[case(None, 0, 0, 0, 0)]
    #[case(Some(1), 0, 0, i64::MAX, 999_999_999)]
    #[case(Some(-1), 0, 0, i64::MIN, -999_999_999)]
    #[case(
        Some(1),
        0,
        i64::MAX / AVERAGE_SECONDS_IN_DAY,
        i64::MAX % AVERAGE_SECONDS_IN_DAY,
        999_999_999,
    )]
    #[case(
        Some(-1),
        0,
        i64::MIN / AVERAGE_SECONDS_IN_DAY,
        i64::MIN % AVERAGE_SECONDS_IN_DAY,
        -999_999_999,
    )]
    #[case(
        Some(1),
        i64::MAX / AVERAGE_SECONDS_IN_MONTH,
        0,
        i64::MAX % AVERAGE_SECONDS_IN_MONTH,
        999_999_999,
    )]
    #[case(
        Some(-1),
        i64::MIN / AVERAGE_SECONDS_IN_MONTH,
        0,
        i64::MIN % AVERAGE_SECONDS_IN_MONTH,
        -999_999_999,
    )]
    #[case(
        Some(1),
        i64::MAX / AVERAGE_SECONDS_IN_MONTH - 2,
        2 * AVERAGE_SECONDS_IN_MONTH / AVERAGE_SECONDS_IN_DAY,
        i64::MAX % AVERAGE_SECONDS_IN_MONTH + 2 * (AVERAGE_SECONDS_IN_MONTH % AVERAGE_SECONDS_IN_DAY),
        999_999_999,
    )]
    #[case(
        Some(-1),
        i64::MIN / AVERAGE_SECONDS_IN_MONTH + 2,
        -2 * AVERAGE_SECONDS_IN_MONTH / AVERAGE_SECONDS_IN_DAY,
        i64::MIN % AVERAGE_SECONDS_IN_MONTH - 2 * (AVERAGE_SECONDS_IN_MONTH % AVERAGE_SECONDS_IN_DAY),
        -999_999_999,
    )]
    fn test_range(
        #[case] tip_over: Option<i8>,
        #[case] months: i64,
        #[case] days: i64,
        #[case] secs: i64,
        #[case] nanos: i32,
    ) {
        assert!(Duration::new(months, days, secs, nanos).is_some());
        let Some(tip_over) = tip_over else {
            return;
        };
        assert_ne!(
            tip_over, 0,
            "tip_over of 0 makes no sense; fix the test case"
        );

        fn assert_overflow(months: i64, days: i64, secs: i64, nanos: i32) {
            assert!(Duration::new(months, days, secs, nanos).is_none());
        }

        if let Some(months) = months.checked_add(i64::from(tip_over)) {
            assert_overflow(months, days, secs, nanos)
        }
        if let Some(days) = days.checked_add(i64::from(tip_over)) {
            assert_overflow(months, days, secs, nanos)
        }
        if let Some(secs) = secs.checked_add(i64::from(tip_over)) {
            assert_overflow(months, days, secs, nanos)
        }
        if let Some(nanos) = i32::from(tip_over)
            .checked_mul(1_000_000_000)
            .and_then(|tip_over| nanos.checked_add(tip_over))
        {
            assert_overflow(months, days, secs, nanos)
        }
    }
}
