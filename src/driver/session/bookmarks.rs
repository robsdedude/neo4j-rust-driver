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

use parking_lot::RwLock;
use std::collections::HashSet;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::result::Result as StdResult;
use std::sync::Arc;

use crate::error::{Neo4jError, Result, UserCallbackError};

type BoxError = Box<dyn StdError + Send + Sync>;
type SupplierFn = Box<dyn Fn() -> StdResult<Arc<Bookmarks>, BoxError> + Send + Sync>;
type ConsumerFn = Box<dyn Fn(Arc<Bookmarks>) -> StdResult<(), BoxError> + Send + Sync>;

#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct Bookmarks {
    bookmarks: HashSet<Arc<String>>,
}

impl Bookmarks {
    pub fn from_raw(raw: impl IntoIterator<Item = String>) -> Self {
        Bookmarks {
            bookmarks: raw.into_iter().map(Arc::new).collect(),
        }
    }

    pub(crate) fn empty() -> Self {
        Bookmarks {
            bookmarks: HashSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.bookmarks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bookmarks.is_empty()
    }

    pub fn into_raw(self) -> impl Iterator<Item = String> {
        self.bookmarks
            .into_iter()
            .map(|bm| Arc::try_unwrap(bm).unwrap_or_else(|bm| String::from(&*bm)))
    }

    pub fn raw(&self) -> impl Iterator<Item = &str> {
        self.bookmarks.iter().map(|bm| bm.as_str())
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

impl Sub for Bookmarks {
    type Output = Bookmarks;

    fn sub(mut self, mut rhs: Self) -> Self::Output {
        if self.bookmarks.len() < rhs.bookmarks.len() {
            std::mem::swap(&mut self.bookmarks, &mut rhs.bookmarks)
        }
        for bm in rhs.bookmarks {
            self.bookmarks.remove(&bm);
        }
        self
    }
}

impl Sub<&Bookmarks> for Bookmarks {
    type Output = Bookmarks;

    fn sub(mut self, rhs: &Bookmarks) -> Self::Output {
        for bm in &rhs.bookmarks {
            self.bookmarks.remove(bm);
        }
        self
    }
}

impl Sub<Bookmarks> for &Bookmarks {
    type Output = Bookmarks;

    fn sub(self, mut rhs: Bookmarks) -> Self::Output {
        for bm in &self.bookmarks {
            rhs.bookmarks.remove(bm);
        }
        rhs
    }
}

impl Sub<&Bookmarks> for &Bookmarks {
    type Output = Bookmarks;

    fn sub(self, rhs: &Bookmarks) -> Self::Output {
        #[allow(clippy::suspicious_arithmetic_impl)]
        Bookmarks {
            bookmarks: self
                .bookmarks
                .difference(&rhs.bookmarks)
                .map(Arc::clone)
                .collect(),
        }
    }
}

impl SubAssign<Bookmarks> for Bookmarks {
    fn sub_assign(&mut self, mut rhs: Bookmarks) {
        if self.bookmarks.len() < rhs.bookmarks.len() {
            std::mem::swap(&mut self.bookmarks, &mut rhs.bookmarks)
        }
        for bm in rhs.bookmarks {
            self.bookmarks.remove(&bm);
        }
    }
}

impl SubAssign<&Bookmarks> for Bookmarks {
    fn sub_assign(&mut self, rhs: &Bookmarks) {
        for bm in &rhs.bookmarks {
            self.bookmarks.remove(bm);
        }
    }
}

pub trait BookmarkManager: Debug + Send + Sync {
    fn get_bookmarks(&self) -> StdResult<Arc<Bookmarks>, BoxError>;
    fn update_bookmarks(
        &self,
        previous: Arc<Bookmarks>,
        new: Arc<Bookmarks>,
    ) -> StdResult<(), BoxError>;
}

pub mod bookmark_managers {
    use super::*;

    type DefaultSupplier = fn() -> StdResult<Arc<Bookmarks>, BoxError>;
    type DefaultConsumer = fn(Arc<Bookmarks>) -> StdResult<(), BoxError>;

    pub const NONE_SUPPLIER: Option<DefaultSupplier> = None;
    pub const NONE_CONSUMER: Option<DefaultConsumer> = None;

    pub fn simple(initial_bookmarks: Option<Arc<Bookmarks>>) -> impl BookmarkManager {
        Neo4jBookmarkManager {
            bookmarks: RwLock::new(initial_bookmarks.unwrap_or_default()),
            supplier: NONE_SUPPLIER,
            consumer: NONE_CONSUMER,
        }
    }

    pub fn with_callbacks<SF, CF>(
        initial_bookmarks: Option<Arc<Bookmarks>>,
        supplier: Option<SF>,
        consumer: Option<CF>,
    ) -> impl BookmarkManager
    where
        SF: Fn() -> StdResult<Arc<Bookmarks>, BoxError> + Send + Sync + 'static,
        CF: Fn(Arc<Bookmarks>) -> StdResult<(), BoxError> + Send + Sync + 'static,
    {
        Neo4jBookmarkManager {
            bookmarks: RwLock::new(initial_bookmarks.unwrap_or_default()),
            supplier,
            consumer,
        }
    }

    pub(crate) fn get_bookmarks(manager: &'_ dyn BookmarkManager) -> Result<Arc<Bookmarks>> {
        manager
            .get_bookmarks()
            .map_err(|err| Neo4jError::UserCallback {
                error: UserCallbackError::BookmarkManagerGet(err),
            })
    }

    pub(crate) fn update_bookmarks(
        manager: &'_ dyn BookmarkManager,
        previous: Arc<Bookmarks>,
        new: Arc<Bookmarks>,
    ) -> Result<()> {
        manager
            .update_bookmarks(previous, new)
            .map_err(|err| Neo4jError::UserCallback {
                error: UserCallbackError::BookmarkManagerUpdate(err),
            })
    }

    struct Neo4jBookmarkManager<SF, CF> {
        bookmarks: RwLock<Arc<Bookmarks>>,
        supplier: Option<SF>,
        consumer: Option<CF>,
    }

    impl<SF, CF> Debug for Neo4jBookmarkManager<SF, CF> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Neo4jBookmarkManager")
                .field("bookmarks", &self.bookmarks)
                .field("supplier", &self.supplier.as_ref().map(|_| "..."))
                .field("consumer", &self.consumer.as_ref().map(|_| "..."))
                .finish()
        }
    }

    impl<SF, CF> Neo4jBookmarkManager<SF, CF> {
        pub fn new() -> Self {
            Self {
                bookmarks: Default::default(),
                supplier: None,
                consumer: None,
            }
        }

        pub fn with_bookmarks(bookmarks: Arc<Bookmarks>) -> Self {
            Neo4jBookmarkManager {
                bookmarks: RwLock::new(bookmarks),
                supplier: None,
                consumer: None,
            }
        }

        pub fn with_bookmark_supplier<SF2>(self, supplier: SF2) -> Neo4jBookmarkManager<SF2, CF>
        where
            SF2: Fn() -> StdResult<Arc<Bookmarks>, BoxError> + Send + Sync + 'static,
        {
            Neo4jBookmarkManager {
                bookmarks: self.bookmarks,
                supplier: Some(supplier),
                consumer: self.consumer,
            }
        }

        pub fn with_bookmark_consumer<CF2>(self, consumer: CF2) -> Neo4jBookmarkManager<SF, CF2>
        where
            CF2: Fn(Arc<Bookmarks>) -> StdResult<(), BoxError> + Send + Sync + 'static,
        {
            Neo4jBookmarkManager {
                bookmarks: self.bookmarks,
                supplier: self.supplier,
                consumer: Some(consumer),
            }
        }
    }

    impl<SF, CF> BookmarkManager for Neo4jBookmarkManager<SF, CF>
    where
        SF: Fn() -> StdResult<Arc<Bookmarks>, BoxError> + Send + Sync + 'static,
        CF: Fn(Arc<Bookmarks>) -> StdResult<(), BoxError> + Send + Sync + 'static,
    {
        fn get_bookmarks(&self) -> StdResult<Arc<Bookmarks>, BoxError> {
            let mut bookmarks = {
                let bookmarks_lock = self.bookmarks.read();
                bookmarks_lock.clone()
            };
            if let Some(supplier) = &self.supplier {
                let supplied_bookmarks = supplier()?;
                bookmarks = Arc::new(&*bookmarks + &*supplied_bookmarks);
            }
            Ok(bookmarks)
        }

        fn update_bookmarks(
            &self,
            previous: Arc<Bookmarks>,
            new: Arc<Bookmarks>,
        ) -> StdResult<(), BoxError> {
            if new.is_empty() {
                return Ok(());
            }
            let mut bookmarks_lock = self.bookmarks.write();
            *bookmarks_lock = Arc::new((**bookmarks_lock).clone() - &*previous + &*new);
            if let Some(consumer) = &self.consumer {
                let bookmarks = bookmarks_lock.clone();
                drop(bookmarks_lock);
                consumer(bookmarks)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    fn bms(bookmarks: Vec<&str>) -> Bookmarks {
        Bookmarks::from_raw(bookmarks.into_iter().map(String::from))
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
