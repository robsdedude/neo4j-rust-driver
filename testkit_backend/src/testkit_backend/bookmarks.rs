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

use std::error::Error as StdError;
use std::result::Result as StdResult;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use neo4j::bookmarks::{bookmark_managers, BookmarkManager, Bookmarks};

use super::backend_id::{BackendId, Generator};
use super::errors::TestKitError;
use super::requests::Request;
use super::responses::Response;
use super::BackendIo;

type BoxError = Box<dyn StdError + Send + Sync>;

pub(super) fn new_bookmark_manager(
    initial_bookmarks: Option<Vec<String>>,
    with_supplier: bool,
    with_consumer: bool,
    backend_io: Arc<AtomicRefCell<BackendIo>>,
    id_generator: Generator,
) -> (BackendId, impl BookmarkManager) {
    let manager_id = id_generator.next_id();
    let initial_bookmarks = initial_bookmarks.map(|bms| Arc::new(Bookmarks::from_raw(bms)));
    let supplier = if with_supplier {
        Some(make_supplier_fn(
            manager_id,
            backend_io.clone(),
            id_generator.clone(),
        ))
    } else {
        None
    };
    let consumer = if with_consumer {
        Some(make_consumer_fn(manager_id, backend_io, id_generator))
    } else {
        None
    };
    let manager = bookmark_managers::with_callbacks(initial_bookmarks, supplier, consumer);
    (manager_id, manager)
}

fn make_supplier_fn(
    manager_id: BackendId,
    backend_io: Arc<AtomicRefCell<BackendIo>>,
    id_generator: Generator,
) -> impl Fn() -> StdResult<Arc<Bookmarks>, BoxError> + Send + Sync {
    move || {
        let mut io = backend_io.borrow_mut();
        let id = id_generator.next_id();
        io.send(&Response::BookmarksSupplierRequest {
            id,
            bookmark_manager_id: manager_id,
        })?;
        let request = io.read_request()?;
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return Err(Box::new(TestKitError::from(err))),
        };
        match request {
            Request::BookmarksSupplierCompleted {
                request_id,
                bookmarks,
            } => {
                if request_id != id {
                    return Err(Box::new(TestKitError::backend_err(format!(
                        "expected BookmarksSupplierCompleted for id {}, received for {}",
                        id, request_id
                    ))));
                }
                Ok(Arc::new(Bookmarks::from_raw(bookmarks)))
            }
            _ => Err(Box::new(TestKitError::backend_err(format!(
                "expected BookmarksSupplierCompleted, received {:?}",
                request
            )))),
        }
    }
}

fn make_consumer_fn(
    manager_id: BackendId,
    backend_io: Arc<AtomicRefCell<BackendIo>>,
    id_generator: Generator,
) -> impl Fn(Arc<Bookmarks>) -> StdResult<(), BoxError> + Send + Sync {
    move |bookmarks| {
        let mut io = backend_io.borrow_mut();
        let id = id_generator.next_id();
        io.send(&Response::BookmarksConsumerRequest {
            id,
            bookmark_manager_id: manager_id,
            bookmarks: bookmarks.raw().map(String::from).collect(),
        })?;
        let request = io.read_request()?;
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return Err(Box::new(TestKitError::from(err))),
        };
        match request {
            Request::BookmarksConsumerCompleted { request_id } => {
                if request_id != id {
                    return Err(Box::new(TestKitError::backend_err(format!(
                        "expected BookmarksConsumerCompleted for id {}, received for {}",
                        id, request_id
                    ))));
                }
                Ok(())
            }
            _ => Err(Box::new(TestKitError::backend_err(format!(
                "expected BookmarksConsumerCompleted, received {:?}",
                request
            )))),
        }
    }
}

#[derive(Debug)]
pub(super) struct TestKitBookmarkManager {
    // id: BackendId,
    // backend_io: Arc<AtomicRefCell<BackendIo>>,
    // id_generator: Generator,
}
