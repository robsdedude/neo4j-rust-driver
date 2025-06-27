# Changelog

⚠️ marks breaking changes or pending breaking changes (deprecations).

## NEXT
***
**👏️ Improvements**
 - Reduce network latency in some cases by setting `TCP_NODELAY`.
 - ⚠️ Reduce the size of `Neo4jError`. The `ServerError` variant is now wrapped in a Box.


## 0.2.0
***
**⭐️ New Features**
 - Add support for Bolt 5.6 (GQL compatible notifications/result statuses)
   - ⚠️ `neo4j::driver::notification::NotificationFilter`'s API has been completely reworked to support this new feature and enable more internal changes in the future without breaking the API again.
   - ⚠️ changed `neo4j::summary::Summary::notifications` from `Option<Vec<Notification>>` to `Vec<Notification>` defaulting to `Vec::new()` when the server does not send any notifications.
 - Add support for Bolt 5.7 (GQL compatible errors)
   - ⚠️ `neo4j::error::ServerError` is now `#[non_exhaustive]`
   - ⚠️ removed `impl From<neo4j::error::ServerError> for neo4j::error::Neo4jError`
   - ⚠️ `neo4j::error::ServerError::new(...)` has been removed.  
     User-code should not need to create arbitrary `ServerError`s.
     In return, `ServerError` now implements `Clone`.
 - Add support for bolt handshake manifest v1.
 - Add support for Bolt 5.8 (home database resolution cache)
   - Includes an optimization where the driver uses a home/default database cache to perform optimistic routing under certain circumstances, saving a full round trip. See the [PR description](https://github.com/robsdedude/neo4j-rust-driver/pull/28) for more details.

**🔧 Fixes**
 - Rework `neo4j::value::graph::Path`
   - `Path`s now properly validate data received from the server (as documented)
   - ⚠️ The return type of `Path::traverse()` was changed to reflect that paths with only one node and no relationships exist.
   - The invariants of `Path` were changed for the above reason, too.
   - New methods `Path::new()`, `Path::new_unchecked()`, and `Path::verify_invariants()`.
 - Fix connection hint `connection.recv_timeout_seconds` should only be applied to reads, not writes.
 - Fix superfluous spaces in bolt debug logs.


## 0.1.0
***
**⭐️ New Features**
 - Add support for Bolt 5.2, which adds notification filtering.
 - Add support for Bolt 5.3 (bolt agent).
 - Add support for Bolt 5.4 (telemetry).
 - Add `Driver::is_encrypted()`.
 - Introduce `neo4j::driver::Conifg::with_keep_alive()` and `without_keep_alive()`.

**👏️ Improvements**
 - ⚠️ ️️Bump `chrono-tz` from `0.8` to `0.9` (types of this crate are exposed through the driver's API).
 - ⚠️ ️️Bump `rustls` from `0.22` to `0.23`: 
   - types of this crate are exposed through the driver's API
   - other breaking changes (e.g., new build requirements).  
     See [rustls' changelog](https://github.com/rustls/rustls/releases/tag/v%2F0.23.0) for more details.

**🔧️ Fixes**
 - Fix `Transaction::rolblack()` failing if a result stream failed before.
 - Fix errors during transaction `BEGIN` not being properly propagated.
 - Fix propagation of `is_retryable()` of errors within transactions.
 - Fix connection hint `connection.recv_timeout_seconds` not always being respected leading to connections timeing out too late.

**🧹️ Clean-up**
 - ⚠️ Remove useless lifetime parameter from `SessionConfig::with_database()`.
 - ⚠️ Change return type of `ConnectionConfig::with_encryption_trust_any_certificate() ` from `Result<Self, Error>` to `Self`.
 - ⚠️ Reduce the number of lifetime generic parameters in `TransactionQueryBuilder` and `TransactionRecordStream`.
 - ⚠️ Remove `impl From<URIError> for ConnectionConfigParseError`.


## 0.0.2
***
**👏 Improvements**
 - Impl `FromStr` for `neo4j::driver::ConnectionConfig` (besides `TryFrom<&str>`).

**🧹️ Clean-up**
 - ⚠️ Update dependencies.  
  Among others `rustls`.
  To accommodate this change, the `rustls_dangerous_configuration` feature was removed.
  This update also affects `ConnectionConfig::with_encryption_custom_tls_config()`, which accepts a
  custom `rustls::ClientConfig`.
 - ⚠️ Make `Record{entries}` private and offer many helper methods instead.
 - Add `EagerResult::into_scalar()`.
 - ⚠️ Renamed `RetryableError` to `RetryError`
 - Fix `Driver::execute_query()::run()` not committing the transaction.
 - ⚠️ Removed `AutoCommitBuilder::without_transaction_timeout` and `AutoCommitBuilder::with_default_transaction_timeout`
  in favor of `AutoCommitBuilder::with_transaction_timeout` in combination with `TransactionTimeout::none`,
  `TransactionTimeout::from_millis` and `TransactionTimeout::default`.  
  Same for `TransactionBuilder`.
 - ⚠️ Move `neo4j::Address` to `neo4j::address::Address`

**📚️ Docs**
 - Much more documentation.


## 0.0.1
***
Initial release
