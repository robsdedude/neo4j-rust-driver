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

use std::collections::HashSet;
use std::ops::{Add, AddAssign};

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct Bookmarks {
    bookmarks: HashSet<String>,
}

impl Bookmarks {
    pub fn from_raw(raw: Vec<String>) -> Self {
        Bookmarks {
            bookmarks: raw.into_iter().collect(),
        }
    }

    pub(crate) fn empty() -> Self {
        Bookmarks {
            bookmarks: HashSet::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.bookmarks.len()
    }

    pub fn into_raw(self) -> impl Iterator<Item = String> {
        self.bookmarks.into_iter()
    }

    pub fn raw(&self) -> impl Iterator<Item = &String> {
        self.bookmarks.iter()
    }
}

impl Add for Bookmarks {
    type Output = Bookmarks;

    fn add(mut self, mut rhs: Self) -> Self::Output {
        if self.bookmarks.len() < rhs.bookmarks.len() {
            std::mem::swap(&mut self.bookmarks, &mut rhs.bookmarks)
        }
        self.bookmarks.extend(rhs.bookmarks);
        self
    }
}

impl Add<&Bookmarks> for Bookmarks {
    type Output = Bookmarks;

    fn add(mut self, rhs: &Bookmarks) -> Self::Output {
        self.bookmarks.extend(rhs.bookmarks.to_owned());
        self
    }
}

impl Add<Bookmarks> for &Bookmarks {
    type Output = Bookmarks;

    fn add(self, mut rhs: Bookmarks) -> Self::Output {
        rhs.bookmarks.extend(self.bookmarks.to_owned());
        rhs
    }
}

impl Add<&Bookmarks> for &Bookmarks {
    type Output = Bookmarks;

    fn add(self, rhs: &Bookmarks) -> Self::Output {
        #[allow(clippy::suspicious_arithmetic_impl)]
        Bookmarks {
            bookmarks: &self.bookmarks | &rhs.bookmarks,
        }
    }
}

impl AddAssign<Bookmarks> for Bookmarks {
    fn add_assign(&mut self, mut rhs: Bookmarks) {
        if self.bookmarks.len() < rhs.bookmarks.len() {
            std::mem::swap(&mut self.bookmarks, &mut rhs.bookmarks)
        }
        self.bookmarks.extend(rhs.bookmarks);
    }
}

impl AddAssign<&Bookmarks> for Bookmarks {
    fn add_assign(&mut self, rhs: &Bookmarks) {
        self.bookmarks.extend(rhs.bookmarks.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    fn bms(bookmarks: Vec<&str>) -> Bookmarks {
        Bookmarks::from_raw(bookmarks.into_iter().map(String::from).collect())
    }

    #[rstest]
    fn bookmarks_add(#[values(true, false)] as_ref: bool) {
        let bm1 = bms(vec!["a", "b"]);
        let bm2 = bms(vec!["b", "c"]);
        let bm3 = bms(vec![]);
        let bm4 = bms(vec!["d"]);

        let bm_sum = if as_ref {
            &bm1 + bm2 + &bm3 + (&bm4 + &bm4)
        } else {
            bm1 + bm2 + bm3 + bm4
        };

        assert_eq!(bm_sum, bms(vec!["a", "b", "c", "d"]));
    }

    #[rstest]
    fn bookmarks_add_assign(#[values(true, false)] as_ref: bool) {
        let mut bm1 = bms(vec!["a", "b"]);
        let bm2 = bms(vec!["b", "c"]);

        if as_ref {
            let bm1_ref = &mut bm1;
            *bm1_ref += bm2;
        } else {
            bm1 += bm2
        }

        assert_eq!(bm1, bms(vec!["a", "b", "c"]));
    }
}
