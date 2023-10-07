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

use std::sync::Arc;

use atomic_refcell::AtomicRefCell;

use crate::address::{AddressResolver, AddressResolverReturn};
use crate::Address;

use super::errors::TestKitError;
use super::requests::Request;
use super::responses::Response;
use super::BackendIo;
use super::Generator;

#[derive(Debug)]
pub(super) struct TestKitResolver {
    backend_io: Arc<AtomicRefCell<BackendIo>>,
    id_generator: Generator,
}

impl TestKitResolver {
    pub(super) fn new(backend_io: Arc<AtomicRefCell<BackendIo>>, id_generator: Generator) -> Self {
        Self {
            backend_io,
            id_generator,
        }
    }
}

impl AddressResolver for TestKitResolver {
    fn resolve(&self, address: Arc<Address>) -> AddressResolverReturn {
        let mut io = self.backend_io.borrow_mut();
        let id = self.id_generator.next_id();
        io.send(&Response::ResolverResolutionRequired {
            id,
            address: address.to_string(),
        })?;
        let request = io.read_request()?;
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return Err(Box::new(TestKitError::from(err))),
        };
        let addresses = match request {
            Request::ResolverResolutionCompleted {
                request_id,
                addresses,
            } => {
                if request_id != id {
                    return Err(Box::new(TestKitError::backend_err(format!(
                        "expected ResolverResolutionCompleted for id {}, received {}",
                        id, request_id
                    ))));
                }
                addresses
            }
            _ => {
                return Err(Box::new(TestKitError::backend_err(format!(
                    "expected ResolverResolutionCompleted, received {:?}",
                    request
                ))))
            }
        };
        Ok(addresses
            .iter()
            .map(String::as_str)
            .map(Address::from)
            .map(Arc::new)
            .collect())
    }
}
