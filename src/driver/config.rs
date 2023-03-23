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

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;

use openssl::error::ErrorStack;
#[cfg(not(test))]
use openssl::ssl::{SslContext, SslContextBuilder};
use openssl::ssl::{SslMethod, SslVerifyMode, SslVersion};
#[cfg(test)]
use tests::{MockSslContext as SslContext, MockSslContextBuilder as SslContextBuilder};
use thiserror::Error;
use uriparse::{Query, URIError, URI};

use crate::address::DEFAULT_PORT;
use crate::{Address, Neo4jError, Result, ValueSend};

const DEFAULT_USER_AGENT: &str = env!("NEO4J_DEFAULT_USER_AGENT");

#[derive(Debug)]
pub struct DriverConfig {
    pub(crate) user_agent: String,
    pub(crate) auth: HashMap<String, ValueSend>, // max_connection_lifetime
    pub(crate) max_connection_pool_size: usize,
    // connection_timeout
    // trust
    // resolver
    // encrypted
    // trusted_certificates
    // ssl_context
    // keep_alive
}

#[derive(Debug)]
pub struct ConnectionConfig {
    pub(crate) address: Address,
    pub(crate) routing_context: Option<HashMap<String, ValueSend>>,
    pub(crate) ssl_context: Option<SslContext>,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            user_agent: String::from(DEFAULT_USER_AGENT),
            auth: HashMap::new(),
            max_connection_pool_size: 100,
        }
    }
}

impl DriverConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_user_agent(mut self, user_agent: String) -> Self {
        self.user_agent = user_agent;
        self
    }

    pub fn with_basic_auth(mut self, user_name: &str, password: &str, realm: &str) -> Self {
        let mut auth = HashMap::with_capacity(3);
        auth.insert("scheme".into(), "basic".into());
        auth.insert("principal".into(), user_name.into());
        auth.insert("credentials".into(), password.into());
        if !realm.is_empty() {
            auth.insert("realm".into(), realm.into());
        }
        self.auth = auth;

        self
    }

    pub fn with_max_connection_pool_size(mut self, max_connection_pool_size: usize) -> Self {
        self.max_connection_pool_size = max_connection_pool_size;
        self
    }
}

impl ConnectionConfig {
    pub fn new(address: Address) -> Self {
        Self {
            address,
            routing_context: Some(HashMap::new()),
            ssl_context: None,
        }
    }

    pub fn with_address(mut self, address: Address) -> Self {
        self.address = address;
        self
    }

    pub fn with_routing(mut self, routing: bool) -> Self {
        if !routing {
            self.routing_context = None
        } else if self.routing_context.is_none() {
            self.routing_context = Some(HashMap::new());
        }
        self
    }

    pub fn with_routing_context(mut self, routing_context: HashMap<String, String>) -> Self {
        self.routing_context = Some(
            routing_context
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        );
        self
    }

    pub fn with_encryption_trust_system_cas(mut self) -> Result<Self> {
        self.ssl_context = Some(Self::secure_ssl_context()?);
        Ok(self)
    }

    pub fn with_encryption_trust_custom_cas<P: AsRef<Path>>(mut self, path: P) -> Result<Self> {
        self.ssl_context = Some(Self::custom_ca_ssl_context(path.as_ref())?);
        Ok(self)
    }

    pub fn with_encryption_trust_any_certificate(mut self) -> Result<Self> {
        self.ssl_context = Some(Self::self_signed_ssl_context()?);
        Ok(self)
    }

    pub fn with_encryption_custom_ssl_context(mut self, ssl_context: SslContext) -> Self {
        self.ssl_context = Some(ssl_context);
        self
    }

    pub fn with_encryption_disabled(mut self) -> Self {
        self.ssl_context = None;
        self
    }

    fn parse_uri(uri: &str) -> Result<ConnectionConfig> {
        let uri = URI::try_from(uri)?;

        let (routing, ssl_context) = match uri.scheme().as_str() {
            "neo4j" => (true, None),
            "neo4j+s" => (true, Some(Self::secure_ssl_context()?)),
            "neo4j+ssc" => (true, Some(Self::self_signed_ssl_context()?)),
            "bolt" => (false, None),
            "bolt+s" => (false, Some(Self::secure_ssl_context()?)),
            "bolt+ssc" => (false, Some(Self::self_signed_ssl_context()?)),
            scheme => {
                return Err(Neo4jError::InvalidConfig {
                    message: format!(
                    "unknown scheme in URI {} expected `neo4j`, `neo4j`, `neo4j+s`, `neo4j+ssc`, \
                         `bolt`, `bolt+s`, or `bolt+ssc`",
                    scheme
                ),
                })
            }
        };

        let authority = uri.authority().ok_or(Neo4jError::InvalidConfig {
            message: String::from("missing host in URI"),
        })?;
        if authority.has_username() {
            return Err(Neo4jError::InvalidConfig {
                message: format!(
                    "URI cannot contain a username, found: {}",
                    authority.username().unwrap()
                ),
            });
        }
        if authority.has_password() {
            return Err(Neo4jError::InvalidConfig {
                message: String::from("URI cannot contain a password"),
            });
        }
        let host = authority.host().to_string();
        let port = authority.port().unwrap_or(DEFAULT_PORT);

        if uri.path() != "/" {
            return Err(Neo4jError::InvalidConfig {
                message: format!("URI cannot contain a path, found: {}", uri.path()),
            });
        }

        let routing_context = match uri.query() {
            None => {
                if routing {
                    Some(HashMap::new())
                } else {
                    None
                }
            }
            Some(query) => {
                if query == "" {
                    Some(HashMap::new())
                } else {
                    if !routing {
                        return Err(Neo4jError::InvalidConfig {
                            message: format!(
                                "URI with bolt scheme cannot contain a query \
                                                  (routing context), found: {}",
                                query,
                            ),
                        });
                    }
                    Some(Self::parse_query(query)?)
                }
            }
        };

        if let Some(fragment) = uri.fragment() {
            return Err(Neo4jError::InvalidConfig {
                message: format!("URI cannot contain a fragment, found: {}", fragment),
            });
        }

        Ok(ConnectionConfig {
            address: (host, port).into(),
            routing_context,
            ssl_context,
        })
    }

    fn parse_query(query: &Query) -> Result<HashMap<String, ValueSend>> {
        let mut result = HashMap::new();
        let mut query = query.to_owned();
        query.normalize();
        for key_value in query.split('&') {
            let mut elements: Vec<_> = key_value.split('=').take(3).collect();
            if elements.len() != 2 {
                return Err(Neo4jError::InvalidConfig {
                    message: format!(
                        "couldn't parse key=value pair '{}' in '{}'",
                        key_value, query
                    ),
                });
            }
            let value = elements.pop().unwrap();
            let key = elements.pop().unwrap();
            result.insert(key.into(), value.into());
        }
        Ok(result)
    }

    fn secure_ssl_context() -> Result<SslContext> {
        let mut context_builder = Self::ssl_context_base()?;
        context_builder.set_default_verify_paths()?;
        Ok(context_builder.build())
    }

    fn custom_ca_ssl_context(path: &Path) -> Result<SslContext> {
        let mut context_builder = Self::ssl_context_base()?;
        context_builder.set_ca_file(path)?;
        Ok(context_builder.build())
    }

    fn self_signed_ssl_context() -> Result<SslContext> {
        let mut context_builder = Self::ssl_context_base()?;
        context_builder.set_verify(SslVerifyMode::NONE);
        Ok(context_builder.build())
    }

    fn ssl_context_base() -> Result<SslContextBuilder> {
        let mut context_builder = SslContextBuilder::new(SslMethod::tls_client())?;
        context_builder.set_min_proto_version(Some(SslVersion::TLS1_2))?;
        Ok(context_builder)
    }
}

impl TryFrom<&str> for ConnectionConfig {
    type Error = Neo4jError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        Self::parse_uri(value)
    }
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ConnectionConfigParseError(String);

impl ConnectionConfigParseError {}

impl From<URIError> for Neo4jError {
    fn from(err: URIError) -> Self {
        Neo4jError::InvalidConfig {
            message: format!("couldn't parse URI {}", err),
        }
    }
}

impl From<ErrorStack> for Neo4jError {
    fn from(err: ErrorStack) -> Self {
        Neo4jError::InvalidConfig {
            message: format!("failed to load SSL config {}", err),
        }
    }
}

#[cfg(test)]
mod tests {
    use mockall::predicate::*;
    use mockall::*;
    use rstest::*;

    use crate::macros::map;

    use super::*;

    #[derive(Debug)]
    pub struct MockSslContext {}

    mock! {
        pub SslContextBuilder {
            pub fn new(method: SslMethod) -> std::result::Result<Self, ErrorStack>;
            pub fn build(self) -> MockSslContext;
            pub fn set_min_proto_version(&mut self, version: Option<SslVersion>) -> std::result::Result<(), ErrorStack>;
            pub fn set_default_verify_paths(&mut self) -> std::result::Result<(), ErrorStack>;
            pub fn set_ca_file(&mut self, file: &Path) -> std::result::Result<(), ErrorStack>;
            pub fn set_verify(&mut self, mode: SslVerifyMode);
        }
    }

    use lazy_static::lazy_static;
    use std::sync::{Mutex, MutexGuard};

    lazy_static! {
        static ref MTX: Mutex<()> = Mutex::new(());
    }

    // When a test panics, it will poison the Mutex. Since we don't actually
    // care about the state of the data we ignore that it is poisoned and grab
    // the lock regardless.  If you just do `let _m = &MTX.lock().unwrap()`, one
    // test panicking will cause all other tests that try and acquire a lock on
    // that Mutex to also panic.
    fn get_ssl_context_mock_lock(m: &'static Mutex<()>) -> MutexGuard<'static, ()> {
        match m.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[rstest]
    fn test_no_tls_by_default() {
        let address = ("localhost", 7687).into();
        let connection_config = ConnectionConfig::new(address);

        assert!(connection_config.ssl_context.is_none());
    }

    #[rstest]
    #[case(None)]
    #[case(Some("bolt://localhost:7687"))]
    #[case(Some("neo4j://localhost:7687"))]
    fn test_no_tls(#[case] uri: Option<&str>) {
        let address = ("localhost", 7687).into();

        let connection_config = match uri {
            None => ConnectionConfig::new(address).with_encryption_disabled(),
            Some(uri) => ConnectionConfig::try_from(uri).unwrap(),
        };

        assert!(connection_config.ssl_context.is_none());
    }

    fn expect_tls(mock: &mut MockSslContextBuilder) {
        mock.expect_set_min_proto_version()
            .with(eq(Some(SslVersion::TLS1_2)))
            .times(1)
            .returning(|_| Ok(()));
        mock.expect_set_default_verify_paths()
            .times(1)
            .returning(|| Ok(()));
        mock.expect_build().times(1).returning(|| SslContext {});
    }

    #[rstest]
    #[case(None)]
    #[case(Some("bolt+s://localhost:7687"))]
    #[case(Some("neo4j+s://localhost:7687"))]
    fn test_tls(#[case] uri: Option<&str>) {
        let _m = get_ssl_context_mock_lock(&MTX);

        let ctx = MockSslContextBuilder::new_context();
        ctx.expect()
            // would require mocking SslMethod
            // .with(predicate::eq(SslMethod::tls_client()))
            .returning(|_| {
                let mut mock = MockSslContextBuilder::default();
                expect_tls(&mut mock);
                Ok(mock)
            });

        let address = ("localhost", 7687).into();

        let connection_config = match uri {
            None => ConnectionConfig::new(address)
                .with_encryption_trust_system_cas()
                .unwrap(),
            Some(uri) => ConnectionConfig::try_from(uri).unwrap(),
        };

        connection_config.ssl_context.unwrap();
    }

    fn expect_self_signed_tls(mock: &mut MockSslContextBuilder) {
        mock.expect_set_min_proto_version()
            .with(eq(Some(SslVersion::TLS1_2)))
            .times(1)
            .returning(|_| Ok(()));
        mock.expect_set_verify()
            .times(1)
            .with(eq(SslVerifyMode::NONE))
            .returning(|_| ());
        mock.expect_build().times(1).returning(|| SslContext {});
    }

    #[rstest]
    #[case(None)]
    #[case(Some("bolt+ssc://localhost:7687"))]
    #[case(Some("neo4j+ssc://localhost:7687"))]
    fn test_self_signed_tls(#[case] uri: Option<&str>) {
        let _m = get_ssl_context_mock_lock(&MTX);

        let ctx = MockSslContextBuilder::new_context();
        ctx.expect()
            // would require mocking SslMethod
            // .with(eq(SslMethod::tls_client()))
            .returning(|_| {
                let mut mock = MockSslContextBuilder::default();
                expect_self_signed_tls(&mut mock);
                Ok(mock)
            });

        let address = ("localhost", 7687).into();
        let connection_config = match uri {
            None => ConnectionConfig::new(address)
                .with_encryption_trust_any_certificate()
                .unwrap(),
            Some(uri) => ConnectionConfig::try_from(uri).unwrap(),
        };

        connection_config.ssl_context.unwrap();
    }

    fn expect_custom_ca_tls(mock: &mut MockSslContextBuilder, path: &'static str) {
        mock.expect_set_min_proto_version()
            .with(eq(Some(SslVersion::TLS1_2)))
            .times(1)
            .returning(|_| Ok(()));
        mock.expect_set_ca_file()
            .times(1)
            .with(eq(Path::new(path)))
            .returning(|_| Ok(()));
        mock.expect_build().times(1).returning(|| SslContext {});
    }

    #[rstest]
    fn test_custom_ca_tls() {
        let _m = get_ssl_context_mock_lock(&MTX);

        let ctx = MockSslContextBuilder::new_context();
        ctx.expect()
            // would require mocking SslMethod
            // .with(eq(SslMethod::tls_client()))
            .returning(|_| {
                let mut mock = MockSslContextBuilder::default();
                expect_custom_ca_tls(&mut mock, "/foo/bar");
                Ok(mock)
            });

        let address = ("localhost", 7687).into();
        let connection_config = ConnectionConfig::new(address)
            .with_encryption_trust_custom_cas("/foo/bar")
            .unwrap();

        connection_config.ssl_context.unwrap();
    }

    #[rstest]
    #[case("neo4j://example.com", true)]
    #[case("bolt://example.com", false)]
    fn test_parsing_routing(#[case] uri: &str, #[case] routing: bool) {
        let connection_config = ConnectionConfig::try_from(uri);
        let connection_config = connection_config.unwrap();
        assert_eq!(connection_config.routing_context.is_some(), routing);
    }

    #[rstest]
    #[case("neo4j://localhost:7687", "localhost")]
    #[case("neo4j://localhost", "localhost")]
    #[case("neo4j://example.com:7687", "example.com")]
    #[case("neo4j://example.com", "example.com")]
    #[case("neo4j://127.0.0.1:7687", "127.0.0.1")]
    #[case("neo4j://127.0.0.1", "127.0.0.1")]
    #[case("neo4j://[::1]:7687", "[::1]")]
    #[case("neo4j://[::1]", "[::1]")]
    #[case("neo4j://localhost:7687?foo=bar", "localhost")]
    #[case("neo4j://localhost?foo=bar", "localhost")]
    #[case("neo4j://example.com:7687?foo=bar", "example.com")]
    #[case("neo4j://example.com?foo=bar", "example.com")]
    #[case("neo4j://127.0.0.1:7687?foo=bar", "127.0.0.1")]
    #[case("neo4j://127.0.0.1?foo=bar", "127.0.0.1")]
    #[case("neo4j://[::1]:7687?foo=bar", "[::1]")]
    #[case("neo4j://[::1]?foo=bar", "[::1]")]
    #[case("bolt://localhost:7687", "localhost")]
    #[case("bolt://localhost", "localhost")]
    #[case("bolt://example.com:7687", "example.com")]
    #[case("bolt://example.com", "example.com")]
    #[case("bolt://127.0.0.1:7687", "127.0.0.1")]
    #[case("bolt://127.0.0.1", "127.0.0.1")]
    #[case("bolt://[::1]:7687", "[::1]")]
    #[case("bolt://[::1]", "[::1]")]
    fn test_parsing_address(#[case] uri: &str, #[case] host: &str) {
        let connection_config = ConnectionConfig::try_from(uri);
        let connection_config = connection_config.unwrap();
        assert_eq!(connection_config.address.host(), host);
    }

    #[rstest]
    #[case("neo4j://localhost", 7687)]
    #[case("neo4j://localhost:7687", 7687)]
    #[case("neo4j://localhost:1337", 1337)]
    #[case("neo4j://example.com", 7687)]
    #[case("neo4j://example.com:7687", 7687)]
    #[case("neo4j://example.com:1337", 1337)]
    #[case("neo4j://127.0.0.1", 7687)]
    #[case("neo4j://127.0.0.1:7687", 7687)]
    #[case("neo4j://127.0.0.1:1337", 1337)]
    #[case("neo4j://[::1]", 7687)]
    #[case("neo4j://[::1]:7687", 7687)]
    #[case("neo4j://[::1]:1337", 1337)]
    #[case("bolt://localhost", 7687)]
    #[case("bolt://localhost:7687", 7687)]
    #[case("bolt://localhost:1337", 1337)]
    #[case("bolt://example.com", 7687)]
    #[case("bolt://example.com:7687", 7687)]
    #[case("bolt://example.com:1337", 1337)]
    #[case("bolt://127.0.0.1", 7687)]
    #[case("bolt://127.0.0.1:7687", 7687)]
    #[case("bolt://127.0.0.1:1337", 1337)]
    #[case("bolt://[::1]", 7687)]
    #[case("bolt://[::1]:7687", 7687)]
    #[case("bolt://[::1]:1337", 1337)]
    fn test_parsing_port(#[case] uri: &str, #[case] port: u16) {
        let connection_config = ConnectionConfig::try_from(uri);
        let connection_config = connection_config.unwrap();
        assert_eq!(connection_config.address.port(), port);
    }

    #[rstest]
    #[case("", map!())]
    #[case("?", map!())]
    #[case("?foo=bar", map!("foo".into() => "bar".into()))]
    #[case("?n=1", map!("n".into() => "1".into()))]
    #[case("?foo=bar&baz=foobar", map!("foo".into() => "bar".into(), "baz".into() => "foobar".into()))]
    fn test_parsing_routing_context(
        #[values(
            "neo4j://localhost:7687",
            "neo4j://localhost",
            "neo4j://example.com:7687",
            "neo4j://example.com",
            "neo4j://127.0.0.1:7687",
            "neo4j://127.0.0.1",
            "neo4j://[::1]:7687",
            "neo4j://[::1]"
        )]
        uri_base: &str,
        #[case] uri_query: &str,
        #[case] routing_context: HashMap<String, ValueSend>,
    ) {
        let uri: String = format!("{}{}", uri_base, uri_query);
        dbg!(&uri, &routing_context);
        let connection_config = ConnectionConfig::try_from(uri.as_str());
        let connection_config = connection_config.unwrap();
        assert_eq!(connection_config.routing_context, Some(routing_context));
    }
}
