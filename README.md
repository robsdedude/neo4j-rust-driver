Neo4j Driver for Rust
=====================

This is a work in progress.

Disclaimer:
 * While I'm currently affiliated with Neo4j, this is my private hobby project.
   It's not an officially supported piece of software by Neo4j.
 * This project is primarily meant for private studies.


Feature Progress:
 * [ ] Well-Written Docs and Examples (**IMPORTANT**)
 * [ ] Driver
   * [x] Config
     * [x] `user_agent`
     * [x] `auth`
       * [x] basic
       * [x] kerberos
       * [x] bearer
       * [x] custom
     * [x] `max_connection_pool_size`
     * [x] `fetch_size`
     * [x] `connection_timeout`
     * [x] `connection_acquisition_timeout`
     * [x] `resolver`
     * [x] routing and direct connections
     * [ ] `keep_alive` (not supported by `std` https://github.com/rust-lang/rust/issues/69774)
     * [x] `TLS`
   * [x] `.session`
   * [x] `.supports_multi_db`
   * [x] `.supports_session_auth`
   * [x] `.execute_query`
   * [x] `.verify_connectivity`
   * [x] `.verify_authentication`
   * [x] `.get_server_info`
   * [ ] `.encrypted` (not sure if needed?)
 * [x] Session
   * [x] Config
     * [x] database
     * [x] bookmarks
     * [x] impersonated_user
     * [x] fetch_size
     * [x] session_auth
   * [x] Auto Commit
   * [x] Transaction
     * [x] Config
       * [x] Metadata
       * [x] Timeout
     * [x] Unmanaged
     * [x] With Retry
 * [ ] Result (`RecordStream`)
   * [x] `.keys`
   * [x] `.consume` (Summary)
     * [x] `.server_info`
       * [x] `.address`
       * [x] `.server_agent`
       * [x] `.protocol_version`
     * [x] `.database`
     * [x] `.query`, `.parameters` (won't implement)
     * [x] `.query_type`
     * [x] `.plan`
     * [x] `.profile`
     * [x] `.notifications`
     * [x] `.counters`
     * [x] `.result_available_after`
     * [x] `.result_consumed_after`
   * [x] `.single`
   * [ ] `.into::<EagerResult>()`
   * [ ] `.closed` (not sure if needed?)
 * [x] Bookmark Management
 * [ ] Protocol Versions
   * [x] 4.4
   * [x] 5.0 (utc fix)
   * [x] 5.1 (re-auth)
   * [ ] 5.2 (notification filtering)
   * [ ] 5.3 (bolt agent)
   * [ ] 5.4 (telemetry)
 * [x] Types
   * [x] `Null`
   * [x] `Integer`
   * [x] `Float`
   * [x] `String`
   * [x] `Boolean`
   * [x] `Bytes`
   * [x] `List`
   * [x] `Map`
   * [x] `Node`
   * [x] `Relationship`
   * [x] `Path`
   * [x] `Spatial` (point)
   * [x] `Temporal` (date, time, datetime, duration)
 * [ ] Proper Unit Tests
 * [ ] CI (https://github.com/actions-rust-lang/setup-rust-toolchain ?)
   * [ ] rustfmt
   * [ ] clippy
   * [ ] Unit Tests
     * [ ] Public API SemVer Compatibility Tests ([cargo-public-api](https://github.com/enselic/cargo-public-api))
   * [ ] TestKit


Future Ideas:
 * `serde` support
   * https://rust-lang.github.io/api-guidelines/interoperability.html#data-structures-implement-serdes-serialize-deserialize-c-serde 
   * https://stackoverflow.com/questions/57474040/serializing-a-sequence-in-serde-when-both-sequence-length-and-types-must-be-know.
