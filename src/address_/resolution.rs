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

use log::debug;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::io::Result as IoResult;
use std::mem;
use std::result::Result as StdResult;
use std::sync::Arc;

use super::Address;
use crate::error::UserCallbackError;
use crate::{Neo4jError, Result};

type BoxError = Box<dyn StdError + Send + Sync>;
pub type AddressResolverReturn = StdResult<Vec<Arc<Address>>, BoxError>;

pub trait AddressResolver: Debug + Send + Sync {
    /// must not return an empty vector
    fn resolve(&self, address: Arc<Address>) -> AddressResolverReturn;
}

pub(crate) fn custom_resolve_address(
    address: Arc<Address>,
    resolver: Option<&dyn AddressResolver>,
) -> Result<Vec<Arc<Address>>> {
    match resolver {
        None => Ok(vec![address]),
        Some(resolver) => custom_resolve(address, resolver),
    }
}

pub(crate) fn resolve_address_fully(
    address: Arc<Address>,
    resolver: Option<&dyn AddressResolver>,
) -> Result<impl Iterator<Item = IoResult<Arc<Address>>>> {
    AddressResolution::new(address, resolver)
}

enum AddressResolution {
    NoResolver {
        dns_buffer: IoResult<Vec<Arc<Address>>>,
    },
    Resolver {
        resolver_buffer: Vec<Arc<Address>>,
        dns_buffer: IoResult<Vec<Arc<Address>>>,
    },
}

impl AddressResolution {
    fn new(address: Arc<Address>, resolver: Option<&dyn AddressResolver>) -> Result<Self> {
        debug!("resolve in: {address:?}");
        match resolver {
            None => {
                let dns_buffer = dns_resolve(address);
                Ok(AddressResolution::NoResolver { dns_buffer })
            }
            Some(resolver) => {
                let resolver_buffer = custom_resolve(address, resolver)?
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>();
                if resolver_buffer.is_empty() {
                    return Err(Neo4jError::InvalidConfig {
                        message: String::from("DriverConfig::resolver returned no addresses."),
                    });
                }
                Ok(AddressResolution::Resolver {
                    resolver_buffer,
                    dns_buffer: Ok(Default::default()),
                })
            }
        }
    }
}

impl Iterator for AddressResolution {
    type Item = IoResult<Arc<Address>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            AddressResolution::NoResolver { dns_buffer } => from_dns_buffer(dns_buffer),
            AddressResolution::Resolver {
                resolver_buffer,
                dns_buffer,
            } => loop {
                if let Some(res) = from_dns_buffer(dns_buffer) {
                    return Some(res);
                }
                if let Some(resolved) = resolver_buffer.pop() {
                    *dns_buffer = dns_resolve(resolved);
                    continue;
                }
                return None;
            },
        }
    }
}

fn custom_resolve(
    address: Arc<Address>,
    resolver: &dyn AddressResolver,
) -> Result<Vec<Arc<Address>>> {
    debug!("custom resolver in: {address}");
    let res = resolver.resolve(address);
    match res {
        Ok(addrs) => {
            debug!(
                "custom resolver out: {:?}",
                addrs.iter().map(|a| format!("{a}")).collect::<Vec<_>>()
            );
            Ok(addrs)
        }
        Err(err) => {
            debug!("custom resolver failed: {:?}", err);
            Err(Neo4jError::UserCallback {
                error: UserCallbackError::ResolverError(err),
            })
        }
    }
}

pub(crate) fn dns_resolve(address: Arc<Address>) -> IoResult<Vec<Arc<Address>>> {
    debug!("dns resolver in: {address}");
    let res = address
        .resolve()
        .map(|addrs| addrs.into_iter().map(Arc::new).collect::<Vec<_>>());
    match &res {
        Ok(addrs) => {
            debug!(
                "dns resolver out: {:?}",
                addrs.iter().map(|a| format!("{a}")).collect::<Vec<_>>()
            );
        }
        Err(err) => {
            debug!("dns resolver out: {:?}", err);
        }
    }
    res
}

fn from_dns_buffer(dns_buffer: &mut IoResult<Vec<Arc<Address>>>) -> Option<IoResult<Arc<Address>>> {
    match dns_buffer {
        Ok(buff) => buff.pop().map(Ok),
        Err(_) => {
            let mut taken_err = Ok(vec![]);
            mem::swap(dns_buffer, &mut taken_err);
            Some(Err(taken_err.unwrap_err()))
        }
    }
}
