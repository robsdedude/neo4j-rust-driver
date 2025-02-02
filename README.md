Neo4j Driver for Rust
=====================

Disclaimer:
 * While I'm currently affiliated with Neo4j, this is my private hobby project.
   It's not an officially supported piece of software by Neo4j.
 * This is a work in progress, and it's mostly developed in my spare time.
 * This project is primarily meant for private studies.
   Yet, I decided to publish it as it might be useful for others as well.


## MSRV
Currently, this crate's minimum supported Rust version is `1.70`.
A bump in MSRV is considered a minor breaking change.


## Feature Progress
 * [x] (Well-Written) Docs and Examples
 * [ ] Driver
   * [ ] Config
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
     * [x] `max_connection_lifetime`
     * [x] `liveness_check_timeout` (called `idle_time_before_connection_test`)
     * [x] routing and direct connections
     * [x] `keep_alive`
     * [x] `TLS`
     * [ ] mTLS for 2FA
   * [x] `.session`
   * [x] `.supports_multi_db`
   * [x] `.supports_session_auth`
   * [x] `.execute_query`
   * [x] `.verify_connectivity`
   * [x] `.verify_authentication`
   * [x] `.get_server_info`
   * [x] `.is_encrypted`
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
   * [ ] Record
     * [x] most basic functionality
     * [ ] ergonomic way to access by key
 * [x] Bookmark Management
 * [ ] Protocol Versions (including features introduced to the official drivers with each version)
   * [x] 4.4
   * [x] 5.0 (utc fix)
   * [x] 5.1 (re-auth)
   * [x] 5.2 (notification filtering)
   * [x] 5.3 (bolt agent)
   * [x] 5.4 (telemetry)
   * [x] 5.5 (never released)
   * [x] 5.6 (GQL notifications)
   * [x] 5.7
     * [x] (GQL errors)
     * [x] (new bolt version handshake)
   * [ ] 5.8 (home db resolution cache)
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
 * [x] CI
   * [x] rustfmt
   * [x] clippy
   * [x] Unit Tests
     * [x] Public API SemVer Compatibility Tests ([cargo-public-api](https://github.com/enselic/cargo-public-api))
   * [x] Exposed Dependency Types Check ([cargo-check-external-types](https://github.com/awslabs/cargo-check-external-types))
   * [x] TestKit


## Note on async
Currently, there are no plans to add async support until a proper abstraction over multiple runtimes is available so that users can choose their preferred runtime.
As it stands, the async ecosystem would either force this crate to dictate a runtime or do an ugly dance to support multiple runtimes.
Even then, the supported runtimes would be limited to the ones chosen by this crate.
