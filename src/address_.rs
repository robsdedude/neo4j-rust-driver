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

use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Result as IoResult;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
use std::vec::IntoIter;

use crate::Result;

pub(crate) mod resolution;

use resolution::{AddressResolver, CustomResolution, DnsResolution};

pub(crate) const DEFAULT_PORT: u16 = 7687;
const COLON_BYTES: usize = ':'.len_utf8();

#[derive(Debug, Clone)]
pub struct Address {
    host: String,
    port: u16,
    key: String,
    pub(crate) is_custom_resolved: bool,
    pub(crate) is_dns_resolved: bool,
}

impl PartialEq for Address {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.port == other.port
    }
}

impl Eq for Address {}

impl Hash for Address {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.port.hash(state);
    }
}

impl Address {
    pub(crate) fn fully_resolve<'resolver>(
        self: Arc<Self>,
        resolver: Option<&'resolver dyn AddressResolver>,
    ) -> Result<impl Iterator<Item = IoResult<Arc<Self>>> + 'resolver> {
        self.custom_resolve(resolver).map(move |addrs| {
            addrs.flat_map(move |a| {
                a.dns_resolve(
                    #[cfg(feature = "_internal_testkit_backend")]
                    resolver,
                )
            })
        })
    }

    pub(crate) fn custom_resolve(
        self: Arc<Self>,
        resolver: Option<&dyn AddressResolver>,
    ) -> Result<impl Iterator<Item = Arc<Self>>> {
        CustomResolution::new(self, resolver)
    }

    pub(crate) fn dns_resolve(
        self: Arc<Self>,
        #[cfg(feature = "_internal_testkit_backend")] resolver: Option<&dyn AddressResolver>,
    ) -> impl Iterator<Item = IoResult<Arc<Self>>> {
        DnsResolution::new(
            self,
            #[cfg(feature = "_internal_testkit_backend")]
            resolver,
        )
    }

    fn normalize_ip(host: &str) -> (bool, String) {
        IpAddr::from_str(host)
            .map(|addr| (true, addr.to_string()))
            .unwrap_or_else(|_| (false, host.to_string()))
    }

    pub fn host(&self) -> &str {
        self.host.as_str()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn unresolved_host(&self) -> &str {
        &self.key
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.host.find(':').is_some() {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl From<(String, u16)> for Address {
    fn from((host, port): (String, u16)) -> Self {
        let (is_resolved, key) = Self::normalize_ip(&host);
        Self {
            host,
            port,
            key,
            is_custom_resolved: false,
            is_dns_resolved: is_resolved,
        }
    }
}

impl From<(&str, u16)> for Address {
    fn from((host, port): (&str, u16)) -> Self {
        let (is_resolved, key) = Self::normalize_ip(host);
        Self {
            host: String::from(host),
            port,
            key,
            is_custom_resolved: false,
            is_dns_resolved: is_resolved,
        }
    }
}

fn parse(host: &str) -> (String, u16) {
    if let Some(pos_colon) = host.rfind(':') {
        if let Some(pos_bracket) = host.rfind(']') {
            if pos_bracket < pos_colon {
                // [IPv6]:port (colon after bracket)
                let port = if let Ok(port) = host[pos_colon + COLON_BYTES..].parse() {
                    port
                } else {
                    DEFAULT_PORT
                };
                (String::from(&host[..pos_colon]), port)
            } else {
                // [IPv6] (bracket after colon)
                (String::from(host), DEFAULT_PORT)
            }
        } else if host[..pos_colon].rfind(':').is_some() {
            // IPv6 (multiple colons)
            (String::from(host), DEFAULT_PORT)
        } else {
            // IPv4:port (single colon)
            let port = if let Ok(port) = host[pos_colon + COLON_BYTES..].parse() {
                port
            } else {
                DEFAULT_PORT
            };
            (String::from(&host[..pos_colon]), port)
        }
    } else {
        // no colon => use default port
        (String::from(host), 7687)
    }
}

impl From<&str> for Address {
    fn from(host: &str) -> Self {
        let (host, port) = parse(host);
        let (is_resolved, key) = Self::normalize_ip(&host);
        Self {
            host,
            port,
            key,
            is_custom_resolved: false,
            is_dns_resolved: is_resolved,
        }
    }
}

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Self {
        Self::from((format!("{}", addr.ip()), addr.port()))
    }
}

impl ToSocketAddrs for Address {
    type Iter = IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        (self.host.as_str(), self.port).to_socket_addrs()
    }
}
