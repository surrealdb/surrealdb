# SurrealDB Security Guide

This document defines the security invariants of SurrealDB and the review triggers
that gate any change touching them. It is the canonical source of truth for what
"secure" means in this codebase.

**Audience.** Contributors and maintainers writing code, human reviewers approving
PRs, and AI tooling assisting with development or code review.

**When to consult.** Before opening or reviewing any change that touches files or
behaviours matched by a section's *Review Triggers* — authentication, sessions,
permissions, RPC/HTTP transport, the parser, function execution, storage keys,
imports, or anything in the *Cross-Cutting Concerns*.

**How to apply.** Treat each invariant as a requirement that must still hold after
the change. If a code path deviates from an invariant, it must carry an explicit
`// SECURITY:` (or equivalent) comment explaining why and why it is safe — see
*Intentional Exceptions*. Otherwise flag the deviation at the severity defined in
*Severity Triage*.

For the general (non-security) review checklist — performance, error handling,
concurrency, test coverage, dependencies — see [REVIEW.md](REVIEW.md).

## Module Locations

File references use short module names. Core engine modules (`iam/`, `dbs/`, `fnc/`,
`buc/`, `rpc/`, `sql/`, `kvs/`, `doc/`) live under `surrealdb/core/src/`. Server
modules (`ntw/`, `rpc/`) live under `surrealdb/server/src/`. When both crates
contain the same module name (e.g. `rpc/`), both locations are security-relevant.

## Review Scope

When a diff touches a security-sensitive area identified by a section's Review
Triggers, the reviewer should:

1. Examine the full diff for the affected files, not just changed lines.
2. Verify that the section's invariants still hold across the changed file, including
   call sites and callers reachable from the changed code.
3. Check whether the change affects other sections (e.g. a change to error types may
   affect Error Sanitization; a change to Options propagation may affect multiple
   sections).

## Severity Triage

When multiple sections are triggered, prioritize findings as follows:

- **Critical**: auth bypass, privilege escalation, data leakage across namespace or
  database isolation boundaries, code execution via user-supplied data.
- **High**: missing or incomplete permission checks, unbounded resource consumption,
  information leakage through error messages, session isolation failures.
- **Medium**: defense-in-depth gaps, missing audit logging, incomplete cleanup,
  overly broad capability defaults.

## Intentional Exceptions

Code paths may intentionally deviate from an invariant with explicit justification.
When reviewing, do not flag a violation if the code contains an accompanying comment
(e.g. `// SAFETY:`, `// SECURITY:`) that documents why the deviation is necessary
and why it is safe. Flag the deviation if the justification is absent, unclear, or
does not address the specific invariant being violated.

Test code (`#[cfg(test)]`, files under `tests/`, `language-tests/`) may use patterns
that are forbidden in production code (e.g. unwrap, perms bypass). These are
acceptable in test context.

---

## Core Security Model

SurrealDB's security model rests on four pillars:

1. **Isolation boundaries**: Namespaces and databases are hard isolation boundaries.
   Data, sessions, and execution context must never cross these boundaries.
2. **Principal hierarchy**: Root > Namespace > Database > Record > Anonymous. Every
   operation must be authorized for the acting principal's level.
3. **Permission enforcement**: Table-level and field-level permissions gate every
   data operation. System-internal paths that suppress permissions (`perms=false`)
   must be strictly bounded.
4. **Capabilities system**: Functions, network access, scripting, and experimental
   features are gated by allow/deny lists.

---

## 1. Authentication & Session Lifecycle

**Files**: `iam/`, `sql/access*.rs`, session/token handling, JWT signing/verification

### Invariants

- Every internal algorithm identifier must map to exactly one JWT library algorithm
  for both signing and verification. The signing-side and verification-side mappings
  must be bijective.
- Roles embedded in JWT claims from access-method tokens must be validated against
  a server-defined allowlist. Absent roles must default to the least-privileged role.
- Token refresh must validate the refresh token against the stored grant before
  trusting any routing claims from the expired access token. Namespace, database,
  and access method must be confirmed by the grant record, not unverified JWT claims.
- Every session and token must have a finite expiration. Missing duration
  configuration must result in a secure default, never unbounded validity.
- Authentication errors must not contain internal query errors, table names, field
  names, or stack traces unless an explicitly-opted-in insecure mode is enabled.
- Bearer token comparison must use constant-time comparison. Grant identifiers must
  correspond to active, non-revoked, non-expired grants in the correct scope.
- JWKS cache entries must be scoped such that a JWK trusted by one
  namespace/database cannot be reused by a different namespace/database, even when
  they reference the same URL.
- SIGNIN, SIGNUP, and AUTHENTICATE clauses must be evaluated with minimum necessary
  privileges. The evaluation session must not grant write access outside the
  authentication flow's scope.
- Session state must only be committed after all authentication steps complete
  successfully. Partial failures must leave the session in its pre-authentication
  state.
- Refresh token revocation and reissuance must be atomic within a single
  transaction. Concurrent reuse must result in at most one successful refresh.

### Review Triggers

Flag for detailed review when changes touch:

- Token claims structure, especially routing fields (NS, DB, AC, ID, RL)
- JWT library integration, algorithm enumeration, or validation defaults
- JWKS caching (key structure, expiration, scope)
- Refresh token rotation (revocation, reissuance, atomicity)
- Session expiration checking in any query execution path
- New RPC methods or HTTP endpoints interacting with session state
- The role model (new roles, new privilege levels, `is_allowed`)

---

## 2. Access Control Enforcement

**Files**: `iam/`, `dbs/options.rs` (`Options` struct, `perms` flag, `with_import`),
`ctx/context.rs` (`check_perms`, `is_allowed`), document processing pipeline in
`doc/` (`check_permissions_table`, `process_table_fields`, `pluck_generic`,
`pluck_select`)

### Invariants

- Every code path that reads, creates, updates, or deletes data on behalf of an
  external actor must invoke permission checks before allowing the operation.
- When system-internal code paths (events, views, cascades) set `perms=false`, this
  suppression must not extend to user-supplied expressions or user-controlled data
  lookups beyond the strictly required scope.
- `OPTION IMPORT` and `OPTION FORCE` must require Editor-or-above at Database scope.
  No Record-scoped or anonymous actor may activate import mode.
- Every `USE` statement that changes namespace or database must verify the actor's
  Auth level grants access to the target scope.
- The `Permission` enum defaults to `Full`. All code constructing permission objects
  must do so intentionally. Omitting a PERMISSIONS clause results in full access.
- Field-level permissions must be enforced on both write path
  (`process_table_fields`) and read path (`pluck_generic`, `pluck_select`),
  including for computed fields, reduced documents, and all output modes (AFTER,
  BEFORE, DIFF, FIELDS).
- Reference cascade operations (ON DELETE CASCADE, UNSET, CUSTOM) must only modify
  records reachable through explicitly defined REFERENCE relationships.
- Permission expressions (WHERE clause in PERMISSIONS) must not produce observable
  side effects (writes, deletes, event triggers).
- The Auth context within Options must not be mutated by user-controlled operations.
  Only system-internal mechanisms (AuthLimit) may produce derived Options with
  modified auth, and these must never broaden permissions.
- The import flag must reset at the start of each new request.
- Record-scoped auth must be verified at all permission check sites to ensure the
  record token's namespace and database match the current execution scope.

### Review Triggers

Flag for detailed review when changes touch:

- `Options::check_perms`, `Options::is_allowed`, or `perms` flag propagation
- `Permission` enum, its `Default` impl, or the `Permissions` struct
- Any new code path that calls `new_with_perms(false)` or `with_import(true)`
- `DEFINE EVENT`, `DEFINE TABLE ... AS`, or `DEFINE FIELD ... REFERENCE` processing
- `Auth`, `AuthLimit`, `Actor`, or role-checking methods
- `USE` statement handling in the executor
- `process_table_fields`, `pluck_generic`, or `pluck_select`
- `purge_references` or cascade processing logic
- How `auth_enabled` is evaluated or propagated
- The `reduced` document mechanism or computed field handling

---

## 3. Execution Context (USE / LET / RESET)

**Files**: Executor, session management, `iam/reset.rs`, context resolution

### Invariants

- Every USE statement, RPC `use` call, and HTTP header-based context selection must
  verify the authenticated principal is authorized for the target namespace and
  database before the switch takes effect.
- Protected parameters (`$auth`, `$token`, `$session`, `$access`) must not be set,
  overwritten, or removed by LET, UNSET, RPC `set`, or RPC `unset`. Protection
  must apply symmetrically to both set and unset operations.
- Session expiry must be checked before executing any statement, processing any RPC
  method, and creating any HTTP request context. No partial execution after expiry.
- Once namespace, database, and session identity are resolved for a statement, they
  must not change in a way that affects already-authorized operations.
- HTTP `surreal-ns` and `surreal-db` headers must be validated against the
  principal's authorized scope.
- Each connection must have an independent session. Session identifiers must not
  collide or be reusable across connections.
- After a `reset`, the session must be in the most restrictive state. If auth is
  required, the reset session must not permit data operations until re-authentication.
- Namespace/database auto-creation via USE in non-strict mode must require the same
  authorization as explicit DEFINE NAMESPACE/DATABASE.
- The execution context must not be observable in a partially-mutated state by any
  concurrent or nested evaluation.

### Review Triggers

Flag when changes touch:

- `USE` statement gains new semantics or session manipulation
- `Session` struct gains new security-relevant fields
- `PROTECTED_PARAM_NAMES` list changes
- RPC methods that manipulate session state
- HTTP header-based context resolution logic
- Session lifecycle (creation, expiration, reset)
- Execution context freezing/unfreezing semantics

---

## 4. Data Write Operations

**Files**: Document processing pipeline (create, update, upsert, insert, delete,
relate), Key API, field evaluation, events, views, cascades

### Invariants

- All write operations must enforce the appropriate table-level permission (create,
  update, or delete) for the authenticated principal.
- Execution context (NS/DB/session) must be resolved at write initiation and cannot
  drift during the document processing pipeline, including retry paths.
- Field-level PERMISSIONS FOR create/update must be evaluated for every field on
  every write with the correct variant based on document state (new vs existing).
- Errors from write operations must not disclose record contents, restricted field
  values, secrets, or internal schema details.
- Any execution triggered during write processing (VALUE, DEFAULT, ASSERT, event
  THEN, view computation, permission WHERE) must use an explicit authority context
  that does not exceed definer intent.
- UPSERT and INSERT operations that fall back from create to update must apply the
  correct permission kind at both table and field levels.
- RELATE must validate that the caller has sufficient access to reference endpoint
  records, or that the edge table's permission policy accounts for this.
- **Key API bodies must be treated as data, never as code.** Request bodies for CRUD
  operations must be interpreted strictly as data values, not as arbitrary SurrealQL
  expressions. Body content must not permit subqueries, function calls, or statement
  execution.
- Cascade operations (ON DELETE CASCADE/UNSET/CUSTOM) must either enforce the
  caller's permissions or require the cascade policy was defined by a sufficiently
  privileged definer.
- Batch write size, transaction duration, and mutation volume must be bounded.
- Event nesting depth must be enforced. View cascade depth and fan-out must be
  limited.
- Import mode (which bypasses field/event/view processing) must only be settable by
  administrative callers.

### Review Triggers

Flag when changes touch:

- Document processing pipeline order (permission check reordering)
- Execution authority semantics for VALUE/DEFAULT/ASSERT/EVENT/VIEW processing
- Cascade/reference behavior (ON DELETE strategies)
- Key API body parsing behavior
- Import mode access control
- New secondary processing triggers (post-write hooks)

---

## 5. HTTP Transport

**Files**: `ntw/` (Axum router, middleware stack), CORS, WebSocket handlers,
GraphQL, error formatting

### Invariants

- CORS must not use wildcard origin when credentialed headers are in the allow-list.
- All request body content entering the Key API must be bound as a variable, never
  passed as an executable expression to the query engine.
- Each HTTP RPC POST request must operate on an isolated session instance. The
  handler must not write per-request session state into a shared slot.
- All WebSocket upgrade paths (SQL and RPC) must apply identical message-size,
  frame-size, and buffer limits. No WebSocket path may accept unbounded messages.
- Client-IP headers must only be trusted when the immediate TCP peer is within a
  configured set of trusted proxy addresses.
- GraphQL execution must enforce configurable maximum query depth, field count, and
  complexity score. Introspection must be gated.
- Error responses must not include raw internal error messages, file paths, storage
  engine details, or stack traces. Internal details must be logged server-side only.
- WebSocket upgrade handlers must validate Origin headers against the same policy
  used by the CORS layer.
- Server identification headers must be suppressed by default.
- Per-endpoint body size limits must be proportional to the endpoint's function.
- When the router is exposed for embedding, the full middleware stack (auth, CORS,
  compression, tracing) must be applied. Individual route modules being `pub` must
  be documented with security implications.

### Review Triggers

Flag when changes touch:

- CORS configuration (origin policy, allowed headers, allowed methods)
- HTTP or WebSocket endpoint routes (add/remove/modify)
- Any path that passes user-supplied content to the query engine
- Session storage in HTTP RPC, `set_session`, or session lifecycle
- WebSocket upgrade handlers, buffer-size or message-limit config
- `client_ip` module (header sources, proxy validation)
- GraphQL service, schema construction, or executor configuration
- Error formatting or `ResponseError` implementation
- Authentication middleware (which endpoints are gated)
- Body-size limit constants or `RequestBodyLimitLayer` application
- Response header middleware (server identification, version)
- Router composition for embedders

---

## 6. RPC Transport

**Files**: `rpc/`, WebSocket handling, CBOR/JSON/FlatBuffers deserialization,
transaction management, notification routing

### Invariants

- CBOR deserialization must enforce a maximum recursion depth during both decoding
  and value conversion. Payloads exceeding the limit must be rejected before
  processing.
- The `begin` RPC method must cap maximum concurrent transactions per WebSocket
  connection. Unbounded transaction accumulation must not be possible.
- WebSocket upgrade must validate the Origin header against an allowlist.
- Default WebSocket message size limits must be conservative (consistent with HTTP
  RPC body size limits, not orders of magnitude larger).
- Memory threshold checks must be applied consistently across all transport entry
  points before full message body deserialization.
- Protected session parameter names must be normalized (lowercase, NFC Unicode,
  trim) before protection checks. The check must apply in all code paths that set
  session variables.
- Each HTTP RPC request must operate on an isolated session copy.
- Live query notification delivery must verify the target WebSocket still exists and
  the live query is still registered to that connection/session.
- Messages in the wrong frame type for the negotiated format must be rejected before
  parsing.
- All open transactions must be explicitly cancelled on WebSocket disconnect.

### Review Triggers

Flag when changes touch:

- CBOR deserialization path, JSON parsing, or FlatBuffers decoding
- WebSocket serve/read/handle_message functions
- RPC dispatch table (new methods, modified signatures)
- Transaction methods (begin/commit/cancel) or transaction map
- Notification routing or LiveQueries map structure
- HTTP RPC handler session management
- `check_protected_param` or `PROTECTED_PARAM_NAMES`
- WebSocket upgrade handler (Origin validation, protocol negotiation)
- Default values for message size, buffer size, or memory threshold
- New serialization formats

---

## 7. Live Query Subscriptions

**Files**: Live query creation, notification dispatch, KILL handling, cleanup

### Invariants

- KILL must verify the requesting principal owns the target subscription before
  deleting it. KILL must be scoped by namespace and database.
- Captured auth/session snapshots must be re-validated before notification delivery.
  Expired tokens and revoked permissions must suppress notifications.
- Per-principal and per-database subscription limits must be enforced.
- Notifications must be delivered exclusively to the session that created the
  subscription. Routing must be resilient to disconnect/reconnect race conditions.
- Live query WHERE clauses must be side-effect free (no writes, deletes, event
  emissions, or external calls).
- Notification backpressure must not cause write transactions to block indefinitely.
- Orphaned subscriptions must be garbage collected.
- Notification delivery failures must be observable (logged, counted).

### Review Triggers

Flag when changes touch:

- KILL authorization or ownership semantics
- Auth/session snapshot validation logic
- Subscription limits or backpressure handling
- Node-level live query key structure
- Notification channel architecture
- Cleanup or garbage collection for subscriptions

---

## 8. Schema Definition (DEFINE / ALTER / REMOVE)

**Files**: Schema statement processing, catalog writes, expression validation

### Invariants

- All schema operations must enforce `is_allowed(Action::Edit, ResourceKind, Base)`
  before execution. Schema operations must require explicit namespace/database
  context.
- Executable expressions in schema definitions (VALUE, ASSERT, COMPUTED, DEFAULT,
  EVENT THEN, PERMISSIONS WHERE) must execute under bounded computation depth and
  explicit authority context. They must not escalate beyond the defining principal's
  privileges.
- PERMISSIONS expressions must be validated syntactically and for basic safety
  before persisting.
- Schema modifications must be fully transactional (commit or rollback, never
  partial state).
- REMOVE EXPUNGE must require elevated privileges.
- ALTER operations that weaken permissions on existing resources require the same
  scrutiny as DEFINE. ALTER is a partial patch and may subtly change security
  posture.
- ALTER FUNCTION, ALTER EVENT, and ALTER API can replace executable code and must be
  treated as equivalent to persistent code injection if authorization is insufficient.
- Import mode that bypasses duplicate checks must only be accessible through
  privileged operations.
- Async event MAXDEPTH and RETRY limits must prevent unbounded background execution.
- Changefeed retention must be bounded.
- Concurrent schema modifications on the same resource must be serialized.

### Review Triggers

Flag when changes touch:

- New DEFINE, ALTER, or REMOVE statement types
- Expression types in VALUE/ASSERT/PERMISSIONS
- Execution authority semantics for schema-defined expressions
- Changefeed, index, or event behavior

---

## 9. Custom API Dispatch

**Files**: `ntw/api.rs`, API routing, middleware, handler execution

### Invariants

- Namespace and database from the URL path (`/api/:ns/:db/:endpoint`) must be
  validated against the session's authenticated context. Mismatches must be rejected.
- Path traversal sequences (`..`, `%2e%2e`, encoded slashes) must be rejected or
  normalized before routing.
- API handlers must execute under an explicit authority context (definer or caller)
  as specified in the API definition. Context must not drift.
- Request data ($request.body, $request.params, $request.query) must not be directly
  interpolated into query strings. Use parameterized queries.
- API errors must be sanitized before response.
- Custom API routing must be gated by capabilities configuration.
- Middleware functions must not be able to modify session authentication state or
  bypass permission evaluations.
- Fallback handlers must be subject to the same permission checks as method-specific
  handlers.

### Review Triggers

Flag when changes touch:

- `ntw/api.rs` routing, path matching, or parameter extraction
- API handler execution context or authority model
- API middleware chain composition or ordering
- Request data binding into query parameters
- API error response formatting
- Capabilities checks for custom API routing
- Fallback handler registration or dispatch

---

## 10. Import / Export

**Files**: Import endpoint, export endpoint, streaming parser, CLI import

### Invariants

- Import must enforce per-statement authorization, not just endpoint-level checks.
  Statements requiring higher privileges (DEFINE USER, DEFINE ACCESS) must be denied
  if the principal lacks those specific permissions.
- OPTION IMPORT (which bypasses field-level schema enforcement) must require
  explicit elevated authorization, and its activation must be logged.
- Import body size and per-statement resource limits must be enforced. The streaming
  parser's buffer growth must be bounded by a configurable maximum.
- USE NS/DB statements within import payloads must be validated against the
  session's authorization scope.
- Import payloads must not be able to create persistent privilege escalation vectors
  (users, access grants, functions at higher privilege than the importer).
- CLI import connecting to a local datastore must not silently disable authentication.
- Export must respect table and field permissions. Secrets must be redacted.

### Review Triggers

Flag when changes touch:

- Import endpoint or streaming parser buffer handling
- `OPTION IMPORT` authorization or flag propagation
- Per-statement authorization during import execution
- Export permission filtering or secret redaction
- CLI import authentication or local datastore connection logic
- USE statement validation within import payloads

---

## 11. Function Execution

**Files**: `fnc/`, scripting runtime (QuickJS), HTTP client, capabilities,
Surrealism/WASM

### Invariants

- JavaScript scripting must be gated by capabilities and denied by default.
- Every function invocation must be checked against the capabilities allow/deny list.
  Deny rules override allow rules.
- Outbound HTTP requests must be checked against network allow/deny lists. Both the
  initial URL and all DNS-resolved IP addresses must be validated. The DNS filtering
  resolver must validate every resolution result.
- Redirect targets must be validated against capabilities.
- JavaScript runtime must enforce memory, stack, and time limits per invocation.
  Each invocation must create a new runtime (no state persistence).
- Crypto compare operations must enforce cost allowance bounds.
- String-producing functions must enforce generation allocation limits.
- Method-style function invocations (e.g., `$array.map(...)`) must be correctly
  canonicalized for capability checking.
- WASM/Surrealism capabilities must be validated before instantiation. WASI context
  configuration (especially `inherit_env`) must be reviewed for secret exposure.

### Review Triggers

Flag when changes touch:

- Function dispatch table (`fnc/mod.rs`)
- JavaScript runtime engine
- WASM runtime, host functions, or WASI configuration
- Scripting or network capability defaults
- HTTP client pooling, DNS resolution, or redirect handling
- New experimental features with function-level gating
- Authority model for stored function execution
- Resource limit defaults or enforcement

---

## 12. File / Bucket Storage

**Files**: `buc/`, `BucketController`, object store backends

### Invariants

- Permission must be checked before every file operation via `check_permission()`
  with the correct `BucketOperation`.
- Permission expressions must not enable access beyond the caller's session
  authority, even when evaluated with permissions disabled for recursion avoidance.
- Object keys must be validated to prevent path traversal. Storage backends must
  enforce prefix isolation.
- Backend error messages must be sanitized. Backend URLs, access keys, and internal
  paths must not appear in client-facing errors.
- Readonly buckets must reject all write operations before any I/O.
- File upload size must be bounded.
- List results must have a maximum count enforced by the system.
- Guest and record users must not be able to list bucket contents.
- Backend URLs in DEFINE BUCKET must be validated against an allowlist.
- Remote object store operations must have bounded timeouts.

### Review Triggers

Flag when changes touch:

- `buc/` module, `BucketController`, or bucket permission checking
- Object key validation or path construction in storage backends
- `DEFINE BUCKET` processing or backend URL handling
- Bucket permission expression evaluation
- File upload size enforcement or list result limits
- Error formatting in bucket operation responses

---

## 13. Metadata & Observability

**Files**: INFO statements, health/status/version endpoints, metrics

### Invariants

- Secrets must be redacted in metadata responses (INFO FOR DB, INFO FOR NS, etc.).
- Observability endpoints must be restricted to authorized principals.
- System information endpoints must not reveal internal architecture details to
  unauthenticated callers.
- Identification headers must be disabled by default.

### Review Triggers

Flag when changes touch:

- INFO statement output formatting or field selection
- Health, status, or version endpoint responses
- Metrics endpoint access control or exported labels
- Secret redaction logic in metadata paths
- Server identification header configuration

---

## 14. Storage Layer (KVS)

**Files**: `kvs/`, key encoding/decoding, transaction handling, datastore
implementations

### Invariants

- All storage keys must be scoped by namespace and database. Key encoding must
  ensure that a key constructed for one namespace/database cannot collide with or
  be interpreted as belonging to a different namespace/database.
- Transaction isolation must be maintained across concurrent operations. Read-after-
  write consistency within a transaction must hold regardless of backend.
- Storage backend errors must be propagated faithfully. Backend-specific error details
  (paths, connection strings, internal state) must be stripped before reaching client-
  facing error paths.
- Key range scans must be bounded. Prefix scans must not inadvertently cross
  namespace or database boundaries due to key encoding ordering.
- Backend credentials and connection configuration must not be logged or included in
  error messages.
- Encryption-at-rest configuration must not be alterable by non-root principals.
- Storage operations triggered by user queries must respect the same authorization
  context as the originating query.

### Review Triggers

Flag when changes touch:

- Key encoding or decoding logic (`kvs/` key types)
- Transaction lifecycle (begin, commit, cancel, retry)
- Datastore initialization or backend selection
- Key prefix construction or range scan boundaries
- Storage-layer error types or error conversion
- New storage backends or backend configuration
- Encryption or credential handling in storage config

---

## 15. Query Parser

**Files**: `syn/`, `sql/`, parser entry points, expression construction

### Invariants

- The parser must enforce maximum nesting depth for all recursive grammar
  productions. Inputs exceeding the limit must be rejected with a bounded-cost error.
- Parser memory consumption must be bounded relative to input size. Buffer growth
  during parsing must have a configurable upper limit.
- The parser must not execute or evaluate any expressions during parsing. Parse output
  must be an inert AST that is only evaluated in the execution phase under proper
  authorization context.
- Parsing untrusted input must not cause panics. All parser entry points must return
  `Result` types.
- Parser-produced AST nodes must faithfully represent the input. Ambiguous syntax
  must be resolved in a way that does not silently change query semantics.

### Review Triggers

Flag when changes touch:

- Parser recursion limits or stack depth configuration
- New grammar productions or expression types
- Parser buffer management or allocation strategy
- Entry points that accept untrusted input strings
- AST node construction for security-relevant statements (DEFINE, OPTION, USE)

---

## Cross-Cutting Concerns

### Confused Deputy Prevention

Any code path where system-internal execution (`perms=false`) processes
user-influenced expressions is a potential confused deputy. This includes:

- Event THEN clauses
- Materialized view maintenance (DEFINE TABLE ... AS)
- Reference cascade operations (ON DELETE CASCADE/UNSET/CUSTOM)
- Permission expression evaluation (WHERE clause in PERMISSIONS)
- VALUE/DEFAULT/ASSERT/COMPUTED field expressions

All such paths must have an explicit, documented authority context. The `perms=false`
scope must be minimized and must not extend to evaluating arbitrary user inputs.

Flag when changes touch `new_with_perms(false)`, authority context propagation in
event/view/cascade processing, or any path that evaluates user-defined expressions
during system-internal operations.

### Error Sanitization

All error responses visible to clients must be sanitized. Review any change to error
formatting, error types, or error propagation paths. Watch for:

- Internal paths or filenames in error messages
- Record contents or field values in error details
- Schema structure details in authorization errors
- Stack traces or panic messages
- Storage engine implementation details
- Backend URLs or credentials

Flag when changes touch error type definitions, `Display`/`ResponseError` impls,
error conversion (`From`/`Into`) chains, or any code path that formats errors for
client responses.

### Resource Bounding

Every operation that processes user-controlled input must have bounded resource
consumption. Watch for:

- Unbounded allocations (parser buffers, batch sizes, list results)
- Missing timeouts (remote calls, long-running queries)
- Amplification patterns (single input triggering multiplicative work)
- Recursion without depth limits
- Connection/transaction accumulation without caps

Flag when changes add new allocation paths for user-controlled data, remove or
increase existing limits, add recursive processing, or introduce new external
call sites without timeouts.

### Import Mode Safety

The `import` flag bypasses field validation, event processing, live query
notifications, and view computation. Any change involving:

- Setting or propagating the `import` flag
- The `OPTION IMPORT` authorization gate
- Document processing pipeline skip conditions

...must be reviewed for authorization correctness and flag lifetime scoping.

Flag when changes touch `with_import`, `OPTION IMPORT` handling, or any document
processing pipeline condition that checks the import flag.

### Namespace/Database Isolation

Every operation must be verified against the principal's authorized namespace and
database. Watch for:

- Context switches without re-authorization
- HTTP headers overriding token-based scope
- Import payloads containing USE statements
- Cross-boundary key structures (keys not scoped by NS/DB)
- Cache entries shared across security boundaries

Flag when changes touch key encoding/decoding, cache key construction, USE statement
handling, HTTP header-based context resolution, or any data structure that is
indexed or partitioned by namespace/database.
