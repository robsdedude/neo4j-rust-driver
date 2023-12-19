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

pub(crate) mod auth;

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use mockall_double::double;
use rustls::ClientConfig;
use thiserror::Error;
use uriparse::{Query, URIError, URI};

use crate::address::{Address, AddressResolver};
use crate::address_::DEFAULT_PORT;
use crate::ValueSend;
use auth::{AuthManager, AuthToken};

const DEFAULT_USER_AGENT: &str = env!("NEO4J_DEFAULT_USER_AGENT");
pub(crate) const DEFAULT_FETCH_SIZE: i64 = 1000;
pub(crate) const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_CONNECTION_ACQUISITION_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct DriverConfig {
    pub(crate) user_agent: String,
    pub(crate) auth: AuthConfig,
    // max_connection_lifetime
    pub(crate) idle_time_before_connection_test: Option<Duration>,
    pub(crate) max_connection_pool_size: usize,
    pub(crate) fetch_size: i64,
    pub(crate) connection_timeout: Option<Duration>,
    pub(crate) connection_acquisition_timeout: Option<Duration>,
    pub(crate) resolver: Option<Box<dyn AddressResolver>>,
    // not supported by std https://github.com/rust-lang/rust/issues/69774
    // keep_alive
}

#[derive(Debug)]
pub(crate) enum AuthConfig {
    Static(Arc<AuthToken>),
    Manager(Arc<dyn AuthManager>),
}

#[derive(Debug)]
pub struct ConnectionConfig {
    pub(crate) address: Address,
    pub(crate) routing_context: Option<HashMap<String, ValueSend>>,
    pub(crate) tls_config: Option<ClientConfig>,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            user_agent: String::from(DEFAULT_USER_AGENT),
            auth: AuthConfig::Static(Default::default()),
            idle_time_before_connection_test: None,
            max_connection_pool_size: 100,
            fetch_size: DEFAULT_FETCH_SIZE,
            connection_timeout: Some(DEFAULT_CONNECTION_TIMEOUT),
            connection_acquisition_timeout: Some(DEFAULT_CONNECTION_ACQUISITION_TIMEOUT),
            resolver: None,
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

    pub fn with_auth(mut self, auth: Arc<AuthToken>) -> Self {
        self.auth = AuthConfig::Static(auth);
        self
    }

    pub fn with_auth_manager(mut self, manager: Arc<dyn AuthManager>) -> Self {
        self.auth = AuthConfig::Manager(manager);
        self
    }

    pub fn with_idle_time_before_connection_test(mut self, idle_time: Duration) -> Self {
        self.idle_time_before_connection_test = Some(idle_time);
        self
    }

    pub fn without_idle_time_before_connection_test(mut self) -> Self {
        self.idle_time_before_connection_test = None;
        self
    }

    pub fn with_max_connection_pool_size(mut self, max_connection_pool_size: usize) -> Self {
        self.max_connection_pool_size = max_connection_pool_size;
        self
    }

    /// fetch_size must be <= i64::MAX
    #[allow(clippy::result_large_err)]
    pub fn with_fetch_size(
        mut self,
        fetch_size: u64,
    ) -> StdResult<Self, ConfigureFetchSizeError<Self>> {
        match i64::try_from(fetch_size) {
            Ok(fetch_size) => {
                self.fetch_size = fetch_size;
                Ok(self)
            }
            Err(_) => Err(ConfigureFetchSizeError { builder: self }),
        }
    }

    pub fn with_fetch_all(mut self) -> Self {
        self.fetch_size = -1;
        self
    }

    pub fn with_default_fetch_size(mut self) -> Self {
        self.fetch_size = DEFAULT_FETCH_SIZE;
        self
    }

    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    pub fn without_connection_timeout(mut self) -> Self {
        self.connection_timeout = None;
        self
    }

    pub fn with_default_connection_timeout(mut self) -> Self {
        self.connection_timeout = Some(DEFAULT_CONNECTION_TIMEOUT);
        self
    }

    pub fn with_connection_acquisition_timeout(mut self, timeout: Duration) -> Self {
        self.connection_acquisition_timeout = Some(timeout);
        self
    }

    pub fn without_connection_acquisition_timeout(mut self) -> Self {
        self.connection_acquisition_timeout = None;
        self
    }

    pub fn with_default_connection_acquisition_timeout(mut self) -> Self {
        self.connection_acquisition_timeout = Some(DEFAULT_CONNECTION_ACQUISITION_TIMEOUT);
        self
    }

    pub fn with_resolver(mut self, resolver: Box<dyn AddressResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    pub fn without_resolver(mut self) -> Self {
        self.resolver = None;
        self
    }
}

impl ConnectionConfig {
    pub fn new(address: Address) -> Self {
        Self {
            address,
            routing_context: Some(HashMap::new()),
            tls_config: None,
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

    #[allow(clippy::result_large_err)]
    pub fn with_routing_context(
        mut self,
        routing_context: HashMap<String, String>,
    ) -> StdResult<Self, InvalidRoutingContextError<Self>> {
        if routing_context.contains_key("address") {
            return Err(InvalidRoutingContextError {
                builder: self,
                it: "cannot contain key 'address'",
            });
        }
        self.routing_context = Some(
            routing_context
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        );
        Ok(self)
    }

    pub fn with_encryption_trust_default_cas(mut self) -> StdResult<Self, TlsConfigError> {
        self.tls_config = Some(match tls_helper::secure_tls_config() {
            Ok(config) => config,
            Err(message) => {
                return Err(TlsConfigError {
                    message,
                    config: self,
                })
            }
        });
        Ok(self)
    }

    pub fn with_encryption_trust_custom_cas<P: AsRef<Path>>(
        self,
        paths: &[P],
    ) -> StdResult<Self, TlsConfigError> {
        fn inner(
            mut config: ConnectionConfig,
            paths: &[&Path],
        ) -> StdResult<ConnectionConfig, TlsConfigError> {
            config.tls_config = Some(match tls_helper::custom_ca_tls_config(paths) {
                Ok(config) => config,
                Err(message) => return Err(TlsConfigError { message, config }),
            });
            Ok(config)
        }
        let paths = paths.iter().map(|path| path.as_ref()).collect::<Vec<_>>();
        inner(self, &paths)
    }

    #[cfg(feature = "rustls_dangerous_configuration")]
    pub fn with_encryption_trust_any_certificate(mut self) -> StdResult<Self, TlsConfigError> {
        self.tls_config = Some(tls_helper::self_signed_tls_config());
        Ok(self)
    }

    pub fn with_encryption_custom_tls_config(mut self, tls_config: ClientConfig) -> Self {
        self.tls_config = Some(tls_config);
        self
    }

    pub fn with_encryption_disabled(mut self) -> Self {
        self.tls_config = None;
        self
    }

    fn parse_uri(uri: &str) -> StdResult<ConnectionConfig, ConnectionConfigParseError> {
        let uri = URI::try_from(uri)?;

        let (routing, tls_config) = match uri.scheme().as_str() {
            "neo4j" => (true, None),
            "neo4j+s" => (true, Some(tls_helper::secure_tls_config()?)),
            "neo4j+ssc" => (true, Some(Self::try_self_signed_tls_config()?)),
            "bolt" => (false, None),
            "bolt+s" => (false, Some(tls_helper::secure_tls_config()?)),
            "bolt+ssc" => (false, Some(Self::try_self_signed_tls_config()?)),
            scheme => {
                return Err(ConnectionConfigParseError(format!(
                    "unknown scheme in URI {} expected `neo4j`, `neo4j`, `neo4j+s`, `neo4j+ssc`, \
                         `bolt`, `bolt+s`, or `bolt+ssc`",
                    scheme
                )))
            }
        };

        let authority = uri
            .authority()
            .ok_or(ConnectionConfigParseError(String::from(
                "missing host in URI",
            )))?;
        if authority.has_username() {
            return Err(ConnectionConfigParseError(format!(
                "URI cannot contain a username, found: {}",
                authority.username().unwrap()
            )));
        }
        if authority.has_password() {
            return Err(ConnectionConfigParseError(String::from(
                "URI cannot contain a password",
            )));
        }
        let host = authority.host().to_string();
        let port = authority.port().unwrap_or(DEFAULT_PORT);

        if uri.path() != "/" {
            return Err(ConnectionConfigParseError(format!(
                "URI cannot contain a path, found: {}",
                uri.path()
            )));
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
                        return Err(ConnectionConfigParseError(format!(
                            "URI with bolt scheme cannot contain a query \
                                                  (routing context), found: {}",
                            query,
                        )));
                    }
                    Some(Self::parse_query(query)?)
                }
            }
        };

        if let Some(fragment) = uri.fragment() {
            return Err(ConnectionConfigParseError(format!(
                "URI cannot contain a fragment, found: {}",
                fragment
            )));
        }

        Ok(ConnectionConfig {
            address: (host, port).into(),
            routing_context,
            tls_config,
        })
    }

    fn parse_query(
        query: &Query,
    ) -> StdResult<HashMap<String, ValueSend>, ConnectionConfigParseError> {
        let mut result = HashMap::new();
        let mut query = query.to_owned();
        query.normalize();
        for key_value in query.split('&') {
            let mut elements: Vec<_> = key_value.split('=').take(3).collect();
            if elements.len() != 2 {
                return Err(ConnectionConfigParseError(format!(
                    "couldn't parse key=value pair '{}' in '{}'",
                    key_value, query
                )));
            }
            let value = elements.pop().unwrap();
            let key = elements.pop().unwrap();
            if key == "address" {
                return Err(ConnectionConfigParseError(format!(
                    "routing context cannot contain key 'address', found: {}",
                    value
                )));
            }
            result.insert(key.into(), value.into());
        }
        Ok(result)
    }

    fn try_self_signed_tls_config() -> StdResult<ClientConfig, ConnectionConfigParseError> {
        #[cfg(feature = "rustls_dangerous_configuration")]
        return Ok(tls_helper::self_signed_tls_config());
        #[cfg(not(feature = "rustls_dangerous_configuration"))]
        return Err(ConnectionConfigParseError(String::from(
            "`neo4j+ssc` and `bolt+ssc` schemes require crate feature \
                `rustls_dangerous_configuration",
        )));
    }
}

impl TryFrom<&str> for ConnectionConfig {
    type Error = ConnectionConfigParseError;

    fn try_from(value: &str) -> StdResult<Self, Self::Error> {
        Self::parse_uri(value)
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
#[error("{message}")]
pub struct TlsConfigError {
    pub message: String,
    pub config: ConnectionConfig,
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct ConnectionConfigParseError(String);

impl ConnectionConfigParseError {}

impl From<URIError> for ConnectionConfigParseError {
    fn from(e: URIError) -> Self {
        ConnectionConfigParseError(format!("couldn't parse URI {e}"))
    }
}

impl From<TlsConfigError> for ConnectionConfigParseError {
    fn from(e: TlsConfigError) -> Self {
        ConnectionConfigParseError(format!("couldn't configure TLS {e}"))
    }
}

impl From<String> for ConnectionConfigParseError {
    fn from(e: String) -> Self {
        ConnectionConfigParseError(e)
    }
}

#[derive(Debug, Error)]
#[error("fetch size must be <= i64::MAX")]
pub struct ConfigureFetchSizeError<Builder> {
    pub builder: Builder,
}

#[derive(Debug, Error)]
#[error("routing context invalid because it {it}")]
pub struct InvalidRoutingContextError<Builder> {
    pub builder: Builder,
    it: &'static str,
}

#[double]
use mockable::tls_helper;

mod mockable {
    #[cfg(test)]
    use mockall::automock;

    #[cfg_attr(test, automock)]
    pub(super) mod tls_helper {
        use rustls::ClientConfig;
        use rustls::{Certificate, RootCertStore};
        use std::fs::File;
        use std::io::{BufReader, Result as IoResult};
        use std::path::Path;
        use std::result::Result as StdResult;
        use std::sync::Arc;
        use std::sync::OnceLock;

        #[cfg(feature = "rustls_dangerous_configuration")]
        use super::NonVerifyingVerifier;

        static SYSTEM_CERTIFICATES: OnceLock<StdResult<Arc<RootCertStore>, String>> =
            OnceLock::new();

        pub fn secure_tls_config() -> StdResult<ClientConfig, String> {
            let root_store = SYSTEM_CERTIFICATES.get_or_init(|| {
                let mut root_store = RootCertStore::empty();
                // root_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
                //     rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                //         ta.subject,
                //         ta.spki,
                //         ta.name_constraints,
                //     )
                // }));
                let native_certs = rustls_native_certs::load_native_certs()
                    .map_err(|e| format!("failed to load system certificates: {e}"))?;
                let (_, _) = root_store.add_parsable_certificates(&native_certs);
                Ok(Arc::new(root_store))
            });
            let root_store = Arc::clone(root_store.as_ref().map_err(Clone::clone)?);
            Ok(ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth())
        }

        #[allow(clippy::needless_lifetimes)] // explicit lifetimes required for automock
        pub fn custom_ca_tls_config<'a, 'b>(
            paths: &'a [&'b Path],
        ) -> StdResult<ClientConfig, String> {
            fn load_certificates_from_pem(path: &Path) -> IoResult<Vec<Certificate>> {
                let file = File::open(path)?;
                let mut reader = BufReader::new(file);
                let certs = rustls_pemfile::certs(&mut reader)?;

                Ok(certs.into_iter().map(Certificate).collect())
            }

            let mut root_store = RootCertStore::empty();
            for path in paths {
                let certs = load_certificates_from_pem(path)
                    .map_err(|e| format!("failed to load certificates from PEM file: {e}"))?;
                for cert in certs.into_iter() {
                    root_store.add(&cert).map_err(|e| {
                        format!("failed to add certificate(s) from {path:?} to root store: {e}")
                    })?;
                }
            }
            Ok(ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth())
        }

        #[cfg(feature = "rustls_dangerous_configuration")]
        pub fn self_signed_tls_config() -> ClientConfig {
            let root_store = RootCertStore::empty();
            let mut config = ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_store)
                .with_no_client_auth();
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(NonVerifyingVerifier {}));
            config
        }
    }

    #[cfg(feature = "rustls_dangerous_configuration")]
    mod dangerous {
        use std::result::Result as StdResult;
        use std::time::SystemTime;

        use rustls::client::{ServerCertVerified, ServerCertVerifier, ServerName};
        use rustls::Certificate;
        use rustls::Error as RustlsError;

        /// As the name suggests, this verifier happily accepts any certificate.
        /// This is not secure and should only be used for testing.
        pub(super) struct NonVerifyingVerifier {}

        impl ServerCertVerifier for NonVerifyingVerifier {
            /// Will verify the certificate is valid in the following ways:
            /// - Signed by a  trusted `RootCertStore` CA
            /// - Not Expired
            /// - Valid for DNS entry
            fn verify_server_cert(
                &self,
                _end_entity: &Certificate,
                _intermediates: &[Certificate],
                _server_name: &ServerName,
                _scts: &mut dyn Iterator<Item = &[u8]>,
                _ocsp_response: &[u8],
                _now: SystemTime,
            ) -> StdResult<ServerCertVerified, RustlsError> {
                Ok(ServerCertVerified::assertion())
            }
        }
    }

    #[cfg(feature = "rustls_dangerous_configuration")]
    use dangerous::NonVerifyingVerifier;
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

    use rustls::RootCertStore;

    use rstest::*;

    use crate::macros::hash_map;

    use super::*;

    static TLS_HELPER_MTX: OnceLock<Mutex<()>> = OnceLock::new();
    // When a test panics, it will poison the Mutex. Since we don't actually
    // care about the state of the data we ignore that it is poisoned and grab
    // the lock regardless.  If you just do `let _m = &MTX.lock().unwrap()`, one
    // test panicking will cause all other tests that try and acquire a lock on
    // that Mutex to also panic.
    fn get_tls_helper_lock() -> MutexGuard<'static, ()> {
        let mutex = TLS_HELPER_MTX.get_or_init(Default::default);
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn get_test_client_config() -> ClientConfig {
        let root_store = RootCertStore::empty();
        ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    }

    #[rstest]
    fn test_no_tls_by_default() {
        let address = ("localhost", 7687).into();
        let connection_config = ConnectionConfig::new(address);

        assert!(connection_config.tls_config.is_none());
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

        assert!(connection_config.tls_config.is_none());
    }

    #[rstest]
    #[case(None)]
    #[case(Some("bolt+s://localhost:7687"))]
    #[case(Some("neo4j+s://localhost:7687"))]
    fn test_tls(#[case] uri: Option<&str>) {
        let _m = get_tls_helper_lock();
        let ctx = tls_helper::secure_tls_config_context();
        ctx.expect().returning(|| Ok(get_test_client_config()));

        let address = ("localhost", 7687).into();

        let connection_config = match uri {
            None => ConnectionConfig::new(address)
                .with_encryption_trust_default_cas()
                .unwrap(),
            Some(uri) => ConnectionConfig::try_from(uri).unwrap(),
        };

        connection_config.tls_config.unwrap();
        // testing the tls_config to be the right on, would require mocking rustls::ClientConfig
    }

    #[rstest]
    #[cfg_attr(feature = "rustls_dangerous_configuration", case(None))]
    #[case(Some("bolt+ssc://localhost:7687"))]
    #[case(Some("neo4j+ssc://localhost:7687"))]
    fn test_self_signed_tls(#[case] uri: Option<&str>) {
        #[cfg(feature = "rustls_dangerous_configuration")]
        {
            let _m = get_tls_helper_lock();
            let ctx = tls_helper::self_signed_tls_config_context();
            ctx.expect().returning(get_test_client_config);

            let address = ("localhost", 7687).into();
            let connection_config = match uri {
                None => ConnectionConfig::new(address)
                    .with_encryption_trust_any_certificate()
                    .unwrap(),
                Some(uri) => ConnectionConfig::try_from(uri).unwrap(),
            };

            connection_config.tls_config.unwrap();
        }
        #[cfg(not(feature = "rustls_dangerous_configuration"))]
        {
            let uri = uri.unwrap();
            let tls_error = ConnectionConfig::try_from(uri).unwrap_err();
            assert!(tls_error
                .to_string()
                .contains("rustls_dangerous_configuration"))
        }
    }

    #[rstest]
    fn test_custom_ca_tls() {
        let test_paths = Arc::new(
            ["/foo", "/bar.pem"]
                .into_iter()
                .map(Path::new)
                .collect::<Vec<_>>(),
        );

        let _m = get_tls_helper_lock();
        let ctx = tls_helper::custom_ca_tls_config_context();
        ctx.expect()
            .withf({
                let test_paths = Arc::clone(&test_paths);
                move |paths| paths == test_paths.deref()
            })
            .returning(|_paths| Ok(get_test_client_config()));

        let address = ("localhost", 7687).into();
        let connection_config = ConnectionConfig::new(address)
            .with_encryption_trust_custom_cas(&test_paths)
            .unwrap();

        connection_config.tls_config.unwrap();
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
    #[case("", hash_map!())]
    #[case("?", hash_map!())]
    #[case("?foo=bar", hash_map!("foo".into() => "bar".into()))]
    #[case("?n=1", hash_map!("n".into() => "1".into()))]
    #[case("?foo=bar&baz=foobar", hash_map!("foo".into() => "bar".into(), "baz".into() => "foobar".into()))]
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
