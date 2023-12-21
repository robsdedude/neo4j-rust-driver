# Changelog

## NEXT
 - Removed `AutoCommitBuilder::without_transaction_timeout` and `AutoCommitBuilder::with_default_transaction_timeout`
   in favor of `AutoCommitBuilder::with_transaction_timeout` in combination with `TransactionTimeout::none`,
   `TransactionTimeout::from_millis` and `TransactionTimeout::default`.
 - Move `neo4j::Address` to `neo4j::address::Address`
 - Impl `FromStr` for `neo4j::driver::ConnectionConfig` (besides `TryFrom<&str>`).
 - Much more documentation

## 0.0.1
Initial release
