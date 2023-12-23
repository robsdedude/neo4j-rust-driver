# Changelog

## NEXT
 - Update dependencies.  
   Among others `rustls`.
   To accommodate this change, the `rustls_dangerous_configuration` feature was removed.
   This update also affects `ConnectionConfig::with_encryption_custom_tls_config()`, which accepts a custom `rustls::ClientConfig`.
 - Make `Record{entries}` private and offer many helper methods instead.
 - Add `EagerResult::into_scalar()`.
 - Renamed `RetryableError` to `RetryError`
 - Fix `Driver::execute_query()::run()` not committing the transaction.
 - Removed `AutoCommitBuilder::without_transaction_timeout` and `AutoCommitBuilder::with_default_transaction_timeout`
   in favor of `AutoCommitBuilder::with_transaction_timeout` in combination with `TransactionTimeout::none`,
   `TransactionTimeout::from_millis` and `TransactionTimeout::default`.  
   Same for `TransactionBuilder`.
 - Move `neo4j::Address` to `neo4j::address::Address`
 - Impl `FromStr` for `neo4j::driver::ConnectionConfig` (besides `TryFrom<&str>`).
 - Much more documentation

## 0.0.1
Initial release
