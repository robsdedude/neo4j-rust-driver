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

use std::io::{Error as IoError, Result as IoResult};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use itertools::Itertools;
use log::error;

use neo4j::address::{Address, AddressResolver, AddressResolverReturn};

use super::errors::TestKitError;
use super::requests::Request;
use super::responses::Response;
use super::BackendIo;
use super::Generator;

#[derive(Debug)]
pub(super) struct TestKitResolver {
    backend_io: Arc<AtomicRefCell<BackendIo>>,
    id_generator: Generator,
    resolver_registered: bool,
    dns_resolver_registered: bool,
}

impl TestKitResolver {
    pub(super) fn new(
        backend_io: Arc<AtomicRefCell<BackendIo>>,
        id_generator: Generator,
        resolver_registered: bool,
        dns_resolver_registered: bool,
    ) -> Self {
        Self {
            backend_io,
            id_generator,
            resolver_registered,
            dns_resolver_registered,
        }
    }

    fn custom_resolve(&self, address: &Address) -> AddressResolverReturn {
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
                        "expected ResolverResolutionCompleted for id {}, received for {}",
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
            .collect())
    }

    fn dns_resolve(&self, address: &Address) -> IoResult<Vec<SocketAddr>> {
        let mut io = self.backend_io.borrow_mut();
        let id = self.id_generator.next_id();
        testkit_to_io_error(io.send(&Response::DomainNameResolutionRequired {
            id,
            name: address.host().to_string(),
        }))?;
        let request = testkit_to_io_error(io.read_request())?;
        let request: Request = match serde_json::from_str(&request) {
            Ok(req) => req,
            Err(err) => return testkit_to_io_error(Err(TestKitError::from(err))),
        };
        let addresses_out = match request {
            Request::DomainNameResolutionCompleted {
                request_id,
                addresses,
            } => {
                if request_id != id {
                    return testkit_to_io_error(Err(TestKitError::backend_err(format!(
                        "expected DomainNameResolutionCompleted for id {}, received for {}",
                        id, request_id
                    ))));
                }
                addresses
            }
            _ => {
                return testkit_to_io_error(Err(TestKitError::backend_err(format!(
                    "expected DomainNameResolutionCompleted, received {request:?}",
                ))))
            }
        };
        addresses_out
            .into_iter()
            .map(|s| (s, address.port()).to_socket_addrs())
            .flatten_ok()
            .collect()
    }
}

fn testkit_to_io_error<T>(res: Result<T, TestKitError>) -> IoResult<T> {
    res.map_err(|err| {
        error!("TestKit messed up DNS resolution request: {err:?}");
        IoError::new(std::io::ErrorKind::Other, err)
    })
}

impl AddressResolver for TestKitResolver {
    fn resolve(&self, address: &Address) -> AddressResolverReturn {
        match self.resolver_registered {
            true => self.custom_resolve(address),
            false => Ok(vec![address.clone()]),
        }
    }

    fn dns_resolve(&self, address: &Address) -> IoResult<Vec<SocketAddr>> {
        match self.dns_resolver_registered {
            true => self.dns_resolve(address),
            false => address.to_socket_addrs().map(|addrs| addrs.collect()),
        }
    }
}
