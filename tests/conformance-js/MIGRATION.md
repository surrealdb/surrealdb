# Rust → JS conformance migration

Status of the migration that replaces the wire-level Rust integration tests
(`tests/*_integration.rs`, plus the wire-observable subset of
`surrealdb/tests/api_integration/`) with JavaScript conformance tests driven
through the SurrealDB JS SDK and raw `fetch` / `WebSocket`.

**Phase: 1–2 (port + parity accounting). NO Rust tests have been deleted.**
This document is the accounting artifact that makes the eventual Phase-3
deletion safe. Deletion happens only after the JS suite has run green in CI for
1–2 weeks (see the deletion manifest at the bottom) and is a separate change.

SDK under test: **surrealdb.js**. Server behavior is pinned against the
current `main`.

---

## 1. Rust file → JS file mapping

| Rust source | Primary JS port | Also covered by (pre-existing JS) | Driver |
| --- | --- | --- | --- |
| `tests/ws_integration.rs` | `tests/conformance-js/tests/websocket.test.ts` | `live.test.ts`, `sessions.test.ts`, `concurrency.test.ts`, `auth.test.ts` | raw JSON-RPC over `WebSocket` (`RpcClient`) |
| `tests/http_integration.rs` | `tests/conformance-js/tests/http.test.ts` | `http.test.ts` | raw `fetch` (+ WS-upgrade headers) |
| `tests/graphql_integration.rs` | `tests/conformance-js/tests/graphql.test.ts` | `graphql.test.ts` | `fetch` POST `/graphql` + `graphql` RPC |
| `tests/gql_integration.rs` | `tests/conformance-js/tests/gql.test.ts` | `gql.test.ts` | `fetch` POST `/gql` + `gql` RPC (`RpcClient`) |
| `surrealdb/tests/api_integration/basic.rs` | `tests/conformance-js/tests/changefeeds.test.ts`, `surrealql.test.ts` | `surrealql-wire.test.ts`, `transactions.test.ts` | SurrealDB JS SDK over `ws` |
| `surrealdb/tests/api_integration/backup.rs` | `tests/conformance-js/tests/backup.test.ts` | — | JS SDK `export()`/`import()` + `/export` `/import` HTTP |
| `tests/cli_integration.rs` (`test_capabilities` only) | `tests/conformance-js/tests/capabilities.test.ts` | `capabilities` also touched by `http.test.ts`/`websocket.test.ts` | raw JSON-RPC (`RpcClient`) |
| `tests/pg_integration.rs` | **(none — DEFERRED)** | — | Postgres wire; not built in the server test binary |

Not in migration scope (left entirely in Rust): `tests/cli_integration.rs`
(the rest — CLI-process behavior), `tests/ml_integration.rs`,
`tests/surrealism_integration.rs`, `tests/database_upgrade.rs`,
`tests/reproduce_issue_*.rs`, and the non-`basic`/`backup` api_integration
files (`live.rs`, `run.rs`, `serialisation.rs`, `session_isolation.rs`,
`version.rs`, `backup_version.rs`).

### KEEP-RUST reason legend

- **protocol-byte** — asserts a binary wire encoding (CBOR / FlatBuffers / gzip)
  a JS/`fetch` driver cannot decode without a forbidden dependency.
- **handshake-header** — needs connection-level request headers (`surreal-id`,
  `x-request-id`, subprotocol) the browser/`bun` `WebSocket` and surrealdb.js
  cannot set.
- **SDK-API** — asserts the typed Rust SDK surface (SurrealValue deserialization,
  builder methods, error-kind enums) rather than server behavior.
- **embedded-engine** — runs the in-process `engine::local` datastore; there is
  no server to drive from JS.
- **CLI-process** — asserts the `surreal` CLI binary / process (stdout, argv
  parsing), not an endpoint.
- **experimental-feature** — gated behind an experimental build/feature not
  enabled in the conformance harness (e.g. `files` buckets, ML, durable
  sessions requiring an on-disk restart).
- **not-yet-ported** — portable in principle, simply out of Phase-1 scope. These
  are NOT deletion-eligible until ported (deleting them would drop coverage).

---

## 2. Parity checklists

### `tests/ws_integration.rs`

Each test runs in a **×3 protocol-format matrix** (`none`, `json`, `cbor`). The
JS port covers the `json` subprotocol only. See the deletion manifest for why a
small Rust CBOR/FlatBuffers smoke must be added before the matrix is deleted.

COVERED → `websocket.test.ts`:

- `ping`, `version`, `info`
- `signup`, `signin`, `invalidate`, `authenticate`
- `letset`, `unset`
- `select`, `insert`, `create`, `update`, `merge`, `patch`, `delete`, `query`
- `run_functions`, `relate_rpc`
- `live_rpc`, `kill`
- `session_reauthentication`
- `session_expiration_operations` (subsumes the base `session_expiration`)
- `rpc_capability`
- `live_query_preserved_on_same_identity_resignin`
- `live_query_cleared_on_principal_change`

COVERED elsewhere (SDK-driven, pre-existing JS):

- `live_query`, `live_second_connection` → `live.test.ts`
- `variable_auth_live_query` → `live.test.ts` (permission-filtered delivery)
- `multi_session_isolation`, `multi_session_authentication`,
  `multi_session_management` → `sessions.test.ts`
- `concurrency` → `concurrency.test.ts`
- `session_use_change_database` → `websocket.test.ts` (`use()` switch)

KEEP-RUST:

- `session_id_defined`, `session_id_defined_generic`, `session_id_defined_both`,
  `session_id_invalid`, `session_id_undefined` — **handshake-header** (session
  id arrives via the `surreal-id` / `x-request-id` connection headers; the JS
  `WebSocket` cannot set them)
- `live_notification_default_session_is_null` — **handshake-header**
- `detach_connection_session_rejected`, `websocket_attach_session_cap` —
  **handshake-header** (raw per-connection `attach`/`detach` framing + a
  `SURREAL_WEBSOCKET_MAX_ATTACHED_SESSIONS` cap; the SDK exercises multiplex
  functionally in `sessions.test.ts` but not this raw cap)
- `live_query_diff` — **not-yet-ported** (LIVE SELECT DIFF patch payloads;
  envelope shape stable but noisy — deprioritized)
- `temporary_directory` — **CLI-process**
- `session_reauthentication_expired`, `session_failed_reauthentication`,
  `session_use_change_database_scope`, `live_query_cleared_on_record_identity_change`,
  `live_table_removal` — **not-yet-ported** (auth/live-query edges beyond the
  Phase-1 slice)

### `tests/http_integration.rs`

COVERED → `http.test.ts`:

- `key_endpoint_rejects_executable_body`
- `rpc_session_hijack_prevention`, `rpc_session_isolation_under_concurrency`
- `http_capabilities`, `arbitrary_query_capabilities`
- `signin_endpoint`
- `client_ip_socket`, `client_ip_none`, `client_ip_extractor`,
  `client_ip_x_forwarded_for`, `client_ip_forwarded_rfc7239`
- `readiness_gate_during_startup_import`, `no_import_server_is_ready_at_bind`
- `session_id`, `no_server_id_headers`
- `sync_endpoint`
- `basic_auth` (level-scoping subset), `bearer_auth` (ns/db-override subset)

COVERED elsewhere (pre-existing `http.test.ts`):

- `health_endpoint`, `version_endpoint`
- `export_endpoint`, `import_endpoint`
- `signup_endpoint` (happy path)
- `rpc_endpoint` (RPC-over-HTTP happy path), `rpc_delete_record_id`
- `key_endpoint_{select,create,update,modify,delete}_{all,one}` (REST /key CRUD)
- the JSON arm of `sql_endpoint`

KEEP-RUST:

- `sql_endpoint` (CBOR / FlatBuffers Accept-negotiation arms),
  `sql_endpoint_with_compression` (gzip `Content-Encoding`),
  `signup_mal` (FlatBuffers Accept / Content-Type) — **protocol-byte**
- `sql_websocket_round_trip` — **handshake-header** (WS-upgrade happy path;
  the deny/subject checks for the upgrade ARE covered in `http.test.ts`)
- `rpc_durable_session_survives_restart`,
  `rpc_durable_session_expires_after_ttl`,
  `rpc_durable_session_expires_while_cached`,
  `rpc_detached_durable_session_cannot_be_resumed`,
  `rpc_sessions_are_not_durable_by_default`,
  `rpc_attach_signup_signin_forwards_bearer` — **experimental-feature /
  embedded-engine** (need `--durable-sessions` + `storage-surrealkv` + a server
  RESTART on the same on-disk path; the harness only spawns fresh in-memory
  servers)
- `bucket_where_permissions_deny_record_users_in_streaming_planner`,
  `bucket_action_and_file_variables_gate_reads_in_streaming_planner`,
  `bucket_action_and_file_variables_gate_writes_in_streaming_planner`,
  `bucket_target_variable_gates_copy_and_rename_in_streaming_planner`,
  `bucket_list_is_denied_for_record_users_in_streaming_planner`,
  `bucket_list_is_denied_for_record_users_in_compute_only_planner` (and the
  bucket helpers) — **experimental-feature** (`--allow-experimental=files` +
  record access + `file::` streaming)
- `experimental_capabilities` — **experimental-feature** (files gating; belongs
  with capability coverage)

### `tests/graphql_integration.rs`

COVERED → `graphql.test.ts` (Rust fn → the porting JS test may fold several
Rust fns into one):

- `basic_auth`, `error_message_safety`
- `auth_mutations`
- `mutation_permissions`, `relation_permissions`, `upsert_permissions`
- `relations`, `record_links`, `functions`, `either_record_conversion`
- `computed_and_readonly_fields_excluded_from_mutation_inputs`,
  `views_have_no_mutation_fields`
- `filters`, `fn_call_filter`, `aggregate_basic_and_groupby`
- `cursor_pagination_connection_field`,
  `cursor_pagination_backward_and_total_count`,
  `cursor_pagination_page_info_is_relay_correct`,
  `cursor_pagination_invalid_cursor_errors`,
  `cursor_pagination_cross_table_cursor_rejected`,
  `connection_field_rejects_order_argument`
- `issue_4555_id_range_and_in_filter`, `id_in_filter_oversize_list_is_rejected`,
  `issue_4554_relation_count_filter_in_where`
- `batched_http_returns_array`
- `issue_4537_graphql_alias_clause`, `graphql_alias_invalid_rejected_at_define`
- `issue_4552_apollo_collision_is_rejected`,
  `issue_4552_collision_with_builtin_rejected`
- `issue_6942_schema_cache_invalidates_on_ddl`,
  `issue_7034_record_in_object_literal`,
  `issue_4999_nested_and_array_record_filter_names`
- `introspection_control`, `depth_and_complexity_limits`,
  `schema_uses_surreal_comments_for_descriptions`,
  `graphql_deprecated_surfaces_in_descriptions`
- `graphql_rpc_method`, `graphql_rpc_with_variables`,
  `graphql_rpc_not_configured`, `graphql_rpc_denied_by_deny_rpc`

COVERED elsewhere (pre-existing `graphql.test.ts`):

- `basic`, `config`, `mutations` (basic create/update/delete),
  `graphql_error_envelope_is_json`

KEEP-RUST:

- `subscriptions_live_query_stream`, `subscriptions_live_query_shape_filter_and_id`,
  `subscriptions_live_query_shape_with_variables` — the `graphql-transport-ws`
  subscription surface. Now covered in `graphql.test.ts` via the `graphql-ws`
  client (surrealdb.js itself exposes no GraphQL subscription API, so a standard
  client drives the upgrade + `connection_init`/`subscribe`/`next` flow).
- `geometry`, `introspection_depth_geometry`, `introspection_depth_nested_array`,
  `vector_similarity_filter`, `vector_knn_filter`, `fulltext_matches_filter`,
  `reserved_word_field_names`, `self_referential_relations`,
  `relation_with_record_link_traversal`, `nested_objects`, `serialization`,
  `cached_record_resolution`, `version`,
  `either_string_literals_with_invalid_identifier_chars`,
  `literal_kind_field_schema_and_mutation`,
  `literal_object_kind_field_schema_and_mutation`,
  `literal_numeric_bool_array_kinds_schema_and_mutation`,
  `issue_4552_apollo_naming_convention` — **not-yet-ported** (portable HTTP
  GraphQL; a natural Phase-2 batch. `serialization` has a CBOR facet that is
  additionally **protocol-byte**.)

### `surrealdb/tests/api_integration/basic.rs` + `backup.rs`

The whole `api_integration` suite is compiled against **`engine::local`**
backends (Mem, RocksDb, SurrealKv, TiKV) and asserts typed Rust SDK values. The
JS port re-pins the **wire-observable** behavior for regression convenience, but
the Rust tests **must remain** — they are the only coverage of the embedded
engine and the typed SDK API. **Deletion-eligible from this file: 0.**

Wire behavior now ALSO covered:

- `backup.rs` (`export_escaped_table_names`, `export_import`, `export_with_config`)
  → `backup.test.ts`
- `basic.rs` `changefeed` → `changefeeds.test.ts`
- `basic.rs` `query`, `query_raw`, `query_binds`, `query_decimals`,
  `select_records_order_by`, `select_records_order_by_start_limit`,
  `select_record_ranges`, `select_records_fetch`, `delete_record_range`,
  `update_table_with_content` → `surrealql.test.ts`

KEEP-RUST — **embedded-engine + SDK-API** (all remaining `basic.rs`):
`connect`, `yuse`, `invalidate`, `signup_record`, `signin_ns`, `signin_db`,
`signin_record`, `record_access_throws_error`, `record_access_invalid_query`,
`authenticate`, `query_with_stats`, `query_chaining`, `mixed_results_query`,
`create_record_*` (5), `insert_table`, `insert_thing`, `insert_relation_table`,
`binding_edges`, `select_table`, `select_record_id`, `update_table`,
`update_record_id`, `update_record_range_with_content`,
`update_record_id_with_content`, `update_merge_record_id`,
`upsert_merge_record_id`, `patch_record_id`, `upsert_patch_record_id`,
`patch_record_id_ops`, `delete_table`, `delete_record_id`, `version`,
`set_unset`, `return_bool`, `multi_take`, `field_and_index_methods`,
`client_side_transactions` (×2), `refresh_tokens`; and `backup.rs`:
`ml_export_import` (also **experimental-feature**).

### `tests/cli_integration.rs` — `test_capabilities` only

COVERED (enforcement) → `capabilities.test.ts` (21 wire tests spanning the
default/deny-all/allow-all/deny-scripting matrices, the function and network
allow/deny precedence matrices — including the "deny wins on match" finding —
and the guest-access matrix).

KEEP-RUST — **CLI-process**: `test_capabilities` should be **trimmed, not
deleted**. The capability *enforcement* is now covered over the wire, but the
Rust test is the only coverage of the `surreal sql` CLI wrapper and
`surreal start --<cap-flag>` argv parsing. Replace the enforcement matrix with a
minimal CLI smoke (one allow + one deny through `surreal sql`) rather than
removing it outright.

### `tests/pg_integration.rs` — DEFERRED (entire file)

The whole module is behind `#[cfg(feature = "postgres")]` and the pg-wire
listener is **not compiled into the server test binary**. There is no endpoint
for the harness to drive. Porting requires a postgres-feature server build (and
a JS Postgres client); tracked separately from this migration. No JS coverage,
no deletion.

---

## 3. Phase 3 deletion manifest

**Nothing here is deleted yet.** This is the checklist to execute in a *separate*
PR once `bun test` for the conformance suite has been green in CI for **1–2
weeks**. Delete only the tests listed as eligible; keep everything under "must
remain".

### Safe to delete (≈94 Rust test fns), once JS is green ≥1–2 weeks

- `ws_integration.rs`: the 26 COVERED fns listed above (`ping`, `version`,
  `info`, `signup`, `signin`, `invalidate`, `authenticate`, `letset`, `unset`,
  `select`, `insert`, `create`, `update`, `merge`, `patch`, `delete`, `query`,
  `run_functions`, `relate_rpc`, `live_rpc`, `kill`, `session_reauthentication`,
  `session_expiration_operations`, `rpc_capability`,
  `live_query_preserved_on_same_identity_resignin`,
  `live_query_cleared_on_principal_change`) — **but see the format-matrix
  caveat below before removing them.**
- `http_integration.rs`: the 18 COVERED fns listed above.
- `gql_integration.rs`: 10 fns — `gql_endpoint_happy_path`,
  `gql_requires_experimental_capability`, `gql_route_can_be_denied`,
  `gql_rejects_invalid_utf8`, `gql_parse_error_shape`, `gql_rpc_method`,
  `gql_rpc_requires_experimental_capability`, `gql_rpc_parse_error_shape`,
  `rpc_gql_unknown_when_method_misspelled`, `gql_rpc_txn_interop`.
- `graphql_integration.rs`: the 40 COVERED fns listed above (plus `basic`,
  `config`, `mutations`, `graphql_error_envelope_is_json`, which are covered by
  the pre-existing `graphql.test.ts`).

### Must REMAIN (do NOT delete)

- **All KEEP-RUST fns** enumerated per file in section 2 (protocol-byte,
  handshake-header, SDK-API, embedded-engine, CLI-process,
  experimental-feature, not-yet-ported).
- **The entire `api_integration` suite** — the JS port re-pins wire behavior but
  the Rust suite is the only coverage of the embedded `engine::local` backends
  and the typed SDK. Deletion-eligible: 0.
- **`gql_integration.rs::gql_accept_negotiation`** — the JS port only pins the
  *observable* HTTP negotiation (status 200 + non-JSON binary body, `text/plain`
  → 415). The full CBOR / FlatBuffers body **decode** assertions must stay
  (protocol-byte).
- **`pg_integration.rs`** — DEFERRED in full.
- **`test_capabilities`** — trim to a CLI smoke; do not delete (CLI-process).
- All shared Rust test helpers still referenced by surviving tests:
  `tests/common/*` (server spawning, `Socket`, `Format`, `StartServerArguments`),
  the `api_integration` `define_include_tests!` / `new_db` machinery, and the
  bucket/durable-session helpers in `http_integration.rs`.

### REQUIRED before deleting the ws ×3 format matrix

Deleting a `ws_integration.rs` test fn removes it from **all three** protocol
modules (`none`, `json`, `cbor`) at once, because each module re-includes the
same fn via `include_tests!`. The JS `RpcClient` speaks only the **`json`**
subprotocol, so removing the ported fns would drop **CBOR** (and, for HTTP,
FlatBuffers/gzip) codec coverage of those RPC methods.

Before deleting the ported ws fns, add a **small Rust format-negotiation smoke
suite** that keeps codec coverage without the full 26×3 matrix — a handful of
representative round-trips (e.g. `signin` + `create` + `select` + a `live`
notification) exercised over **CBOR** and **FlatBuffers**, plus the HTTP
`sql_endpoint` CBOR/FlatBuffers arms and the gzip `sql_endpoint_with_compression`
case (which are already flagged KEEP-RUST as protocol-byte and should be the
seed of this smoke suite). Only after that smoke exists is the ws JSON-arm
matrix safe to remove.
