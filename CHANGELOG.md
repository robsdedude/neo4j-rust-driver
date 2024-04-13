# Changelog

⚠️ marks breaking changes or pending breaking changes (deprecations).

## NEXT
***
**⭐ New Features**
- Add support for Bolt 5.2, which adds notification filtering.
- Add support for Bolt 5.3 (bolt agent).
- Add support for Bolt 5.4 (telemetry).
- Add `Driver::is_encrypted()`.
- Introduce `neo4j::driver::Conifg::with_keep_alive()` and `without_keep_alive()`.

**🔧 Fixes**
- Fix `Transaction::rolblack()` failing if a result stream failed before.
- Fix errors during transaction `BEGIN` not being properly propagated.
- Fix propagation of `is_retryable()` of errors within transactions.

**🧹Clean-up**
- ⚠️ Removed useless lifetime parameter from `SessionConfig::with_database()`.
- ⚠️ Changed return type of `ConnectionConfig::with_encryption_trust_any_certificate() ` from `Result<Self, Error>` to `Self`.
- ⚠️ Reduce the number of lifetime generic parameters in `TransactionQueryBuilder` and `TransactionRecordStream`.


## 0.0.2
***
**👏 Improvements**
- Impl `FromStr` for `neo4j::driver::ConnectionConfig` (besides `TryFrom<&str>`).

- **🧹Clean-up**
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

**📚 Docs**
- Much more documentation.

## 0.0.1

Initial release
