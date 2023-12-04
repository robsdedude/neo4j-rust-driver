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

use log::warn;
use std::result::Result as StdResult;
use std::thread::sleep;
use std::time::Duration;

use rand;
use rand::Rng;
use thiserror::Error;

use crate::time::Instant;
use crate::{Neo4jError, Result};

pub trait RetryPolicy {
    type Error;

    fn execute<R>(&self, work: impl FnMut() -> Result<R>) -> StdResult<R, Self::Error>;
}

#[derive(Debug, Clone, Copy)]
pub struct ExponentialBackoff {
    initial_delay: Duration,
    max_retry_time: Duration,
    factor: f64,
    jitter: f64,
}

#[derive(Error, Debug)]
pub enum RetryableError {
    #[error("Non-retryable error occurred: {0}")]
    Neo4jError(#[from] Neo4jError),
    #[error("{0}")]
    Timeout(#[from] TimeoutError),
}

#[derive(Error, Debug)]
#[error("Timeout occurred while retrying. Last error: {}", .errors.last().unwrap())]
pub struct TimeoutError {
    errors: Vec<Neo4jError>,
}

impl From<TimeoutError> for Vec<Neo4jError> {
    fn from(value: TimeoutError) -> Self {
        value.errors
    }
}

impl ExponentialBackoff {
    fn max_retries(&self) -> usize {
        /*
        1 * 0.8
        + 1 * 2 * 0.8
        + 1 * 2 * 2 * 0.8
        + ...
        = 1 * 0.8 * (2^0 + 2^1 + 2^2 + ... + 2^x)
        = 1 * 0.8 * (2^(x+1) - 1)

        init * min_jitter * (factor ^ (x + 1) - 1) <= max_retries

        <=> x <= log factor (max_retries / (init * min_jitter) + 1) - 1

         */
        assert!(self.initial_delay > Duration::ZERO);
        assert!(self.max_retry_time > Duration::ZERO);
        let max_time = self.max_retry_time.as_secs_f64();
        let init = self.initial_delay.as_secs_f64();
        let min_jitter = self.factor * self.jitter;
        ((1.0 + max_time / (init * min_jitter)).log(self.factor) - 1.0).ceil() as usize
    }

    pub fn with_max_retry_time(self, max_retry_time: Duration) -> Self {
        Self {
            max_retry_time,
            ..self
        }
    }
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_retry_time: Duration::from_secs(30),
            factor: 2.0,
            jitter: 0.2,
        }
    }
}

impl RetryPolicy for &ExponentialBackoff {
    type Error = RetryableError;

    fn execute<R>(&self, mut work: impl FnMut() -> Result<R>) -> StdResult<R, Self::Error> {
        assert!(self.jitter >= 0.0);
        assert!(self.jitter < 1.0);
        let mut time_start = None;
        let mut errors = None;
        let mut current_delay = self.initial_delay.as_secs_f64();
        let mut rng = rand::thread_rng();
        loop {
            let res = work();
            if time_start.is_none() {
                time_start = Some(Instant::now());
            }
            let err = match res {
                Err(e) if e.is_retryable() => e,
                _ => return res.map_err(Into::into),
            };
            if errors.is_none() {
                errors = Some(Vec::with_capacity(self.max_retries()));
            }
            errors.as_mut().unwrap().push(err);
            let time_elapsed = time_start.unwrap().elapsed();
            if time_elapsed > self.max_retry_time {
                return Err(TimeoutError {
                    errors: errors.unwrap(),
                }
                .into());
            }
            let jitter_factor = 1.0 + rng.gen_range(-self.jitter..=self.jitter);
            let jittered_delay = current_delay * jitter_factor;
            warn!(
                "Transaction failed and will be retired in {:.4} seconds: {}",
                jittered_delay,
                errors.as_ref().unwrap().last().unwrap()
            );
            sleep(Duration::try_from_secs_f64(jittered_delay).unwrap_or(Duration::MAX));
            current_delay *= self.factor;
        }
    }
}

impl RetryPolicy for ExponentialBackoff {
    type Error = <&'static ExponentialBackoff as RetryPolicy>::Error;

    fn execute<R>(&self, work: impl FnMut() -> Result<R>) -> StdResult<R, Self::Error> {
        (&self).execute(work)
    }
}
