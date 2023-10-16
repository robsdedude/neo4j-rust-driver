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
   * [ ] Config
     * [x] `user_agent`
     * [ ] `auth`
       * [x] basic
       * [ ] kerberos
       * [ ] bearer
       * [ ] custom
     * [x] `max_connection_pool_size`
     * [x] `fetch_size`
     * [x] `connection_timeout`
     * [x] `connection_acquisition_timeout`
     * [x] `resolver`
     * [x] routing and direct connections
     * [ ] `keep_alive`
     * [x] `TLS`
   * [x] `.session`
   * [x] `.supports_multi_db`
   * [ ] `.execute_query`
   * [ ] `.verify_connectivity`
   * [ ] `.get_server_info`
   * [ ] `.encrypted` (not sure if needed?)
 * [x] Session
   * [x] Config
     * [x] database
     * [x] bookmarks
     * [x] impersonated_user
     * [x] fetch_size
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
 * [ ] Bookmark Management
 * [ ] Protocol Versions
   * [x] 4.4
   * [x] 5.0 (utc fix)
   * [ ] 5.1 (re-auth)
   * [ ] 5.2 (notification filtering)
   * [ ] 5.3 (bolt agent)
   * [ ] 5.4 (telemetry)
 * [ ] Types
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
   * [ ] `Temporal` (date, time, datetime, duration) (**IMPORTANT**)
 * [ ] Proper Unit Tests
 * [ ] CI
   * [ ] rustfmt
   * [ ] clippy
   * [ ] Unit Tests
   * [ ] TestKit


Future Ideas:
 * `serde` support
   * https://stackoverflow.com/questions/57474040/serializing-a-sequence-in-serde-when-both-sequence-length-and-types-must-be-know.
