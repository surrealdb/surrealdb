# JS conformance suite

Server-behavior conformance tests driven through the JavaScript SDK. This is
**not** an SDK test suite: the SDK is the driver, the server is the subject.
It covers connection-level behavior that `.surql` language tests cannot
express — authentication (record, BEARER, external JWT, the `AUTHENTICATE`
clause) and refresh-token lifecycle, live queries, session semantics (including
multiplexed sessions), transactions and concurrent-writer conflicts, capability
gating, schema-constraint enforcement (`DEFINE FIELD`/`INDEX`/`EVENT`),
full-text and vector search, wire-level value mapping, and the HTTP, GraphQL,
ISO GQL, and custom-API endpoints (driven with raw `fetch` where the SDK has no
surface for them).

## Running

Requires `bun` and a `surreal` binary on the PATH (override with
`SURREAL_BIN=/path/to/surreal`).

```sh
bun install
bun test
```

Each test file spawns its own in-memory server on a random port; each test
uses a unique namespace/database. No external network access, no shared
state — files and tests are parallel-safe.

### Target binary

The suite pins the behavior of the current engine. Point `SURREAL_BIN` at a
build of the current `main` — some tests assert behavior (conflict
classification, live-notification delivery, ISO GQL enabled by default) that has
landed on `main` but is not yet in a stable release.

## Conventions

- Assertions pin **observed server behavior** (results accepted from the
  current release, like the language-tests corpus). Where behavior is
  surprising, a comment says so rather than the test asserting what we wish
  happened — e.g. role-denied data writes are *silently filtered* (empty
  result, no error) while role-denied schema changes reject loudly.
- A known limitation gets a comment plus a `test.skip`'d correct-behavior test
  — the acceptance test for the eventual fix. Warts that may be by design are
  pinned as observed with a comment instead.
- Negative cases use bounded silence windows (`assertSilence`), positive
  cases use event-driven waits with generous timeouts — never bare sleeps
  for positive assertions.
- `.json()` result mapping: SurrealQL `NONE` surfaces as `undefined`.
- Live subscriptions require `new Table("name")` — a plain string is
  silently accepted by the SDK at runtime but subscribes to nothing.
- Harness trap: pass variadic capability flags to `startServer({ args })` in
  the `--flag=value` form (`--deny-http=graphql`, `--allow-eval-query=system`).
  The bare two-token form makes clap's greedy variadic parser swallow the
  trailing `memory` positional and the server fails to boot.
- In-transaction pauses must be `LET $x = sleep(..)` — `RETURN sleep(..)`
  inside `BEGIN..COMMIT` ends the transaction early (and still commits).

## Known findings

- `tests/concurrency.test.ts` has a `test.skip` documenting **lost updates**:
  write-write conflict detection intermittently misses under two-connection
  contention (some rounds commit two transactions that read the same base
  value; rarely, concurrent same-id CREATE rounds report BOTH creates
  successful). Client-side `.retry()` cannot paper over it.
- Pinned warts (observed-behavior pins with comments, adjudication
  candidates rather than skipped bugs):
  - Access JWTs are stateless: refresh rotation, grant revocation, and
    `invalidate()` never invalidate a previously issued access token before
    its own expiry, and only root SurrealQL (`ACCESS ... REVOKE GRANT`) kills
    a refresh token (`auth-refresh`).
  - Refresh reuse/revocation/expiry all yield one generic authentication
    error, but redeeming after `REMOVE ACCESS` leaks the specific "access
    method does not exist" to unauthenticated callers (`auth-refresh`).
  - `ACCESS ... REVOKE GRANT` returns its grants doubly nested (`[[grant]]`)
    while `SHOW ALL` returns a flat array (`auth-refresh`).
  - `/export` redacts access `KEY`s to the literal `'[REDACTED]'`, so an
    export/import round-trip silently breaks verification of tokens issued
    by the source database (`http`).
  - Inconsistent `/sql` auth failures: anonymous → 403 JSON problem doc,
    wrong basic-auth → 401 bare text/plain; HTTP `/signin` accepts only the
    legacy `{user, pass}` keys; wrong record-access credentials → 404
    (`http`).
  - GraphQL: the per-table singular field takes the bare record key and
    silently returns `null` for the full `table:key` id the API itself
    emits, while `_get` requires the full form; anonymous denial surfaces as
    HTTP 200 + "Internal Error: ..." in the errors array (`graphql`).
  - ISO GQL is enabled by default (the `--allow-experimental=gql` flag remains
    a no-op accepted for backwards compatibility). `eval::gql` collapses a
    single-row result to the bare object, and `/gql` binds `$vars` from URL
    query parameters (`gql`).
  - `$auth`/`$session`/`$token` bindings are rejected as protected, but
    `$this`/`$parent` are accepted as wire bindings (document context still
    shadows them per-record) (`surrealql-wire`).
  - surrealdb.js bundles two copies of its value/error classes:
    `RecordId.equals` across copies throws "Cannot access invalid private
    field", and server errors fail `instanceof` against package-root classes
    (`surrealql-wire` — an SDK bundling bug, noted in passing; the server
    behaves correctly).
  - Changefeeds: `SHOW CHANGES FOR TABLE ... SINCE <versionstamp>` is
    INCLUSIVE of the supplied versionstamp — the change AT the boundary is
    re-delivered, so an incremental poller must dedup (or poll `SINCE last+1`).
    Also, `CREATE`s surface in the feed as `update` actions (not `create`), and
    the `DEFINE TABLE` itself is recorded as the first feed entry
    (`changefeeds`).
  - GraphQL: a permission-denied `deleteX` mutation returns `true` (no error)
    even though nothing is deleted, and `deleteManyX` returns `0` — the
    silently-filtered pattern that role-denied data writes follow, surfaced
    through the resolver (`graphql`).
  - Capabilities: precedence is "deny wins on match" — a broad `--deny-funcs`
    (or `--deny-net`) target is NOT rescued by a strictly more specific
    competing `--allow-...`; a target runs only when some allow matches AND no
    deny matches. Function/network denials arrive as a per-statement `ERR`
    envelope (`kind: "NotAllowed"`, `details.kind` `Function`/`Target`) inside a
    successful `query` RPC, whereas an anonymous guest denial is a TOP-LEVEL
    JSON-RPC error (code `-32002`) (`capabilities`).
  - `eval::surql` / `eval::gql` are denied for every subject by default and are
    NOT rescued by `--allow-all`; they need `--allow-eval-query=<subject>` on
    top of the arbitrary-query gate. Once the eval gate passes, `eval::gql`
    reaches the GQL parser, so a query with no `MATCH` clause fails there with a
    kind `Internal` parse error, not a capability denial (`capabilities`).
  - The arbitrary-query subject gate (`--deny-arbitrary-query=system`) denies
    the RPC verbs themselves — `query`, the CRUD verbs, `run`, and even `use` —
    with a TOP-LEVEL `-32602` "Method not allowed" (the same surface as
    `--deny-rpc`), while `ping`/`version`/`signin` still run (`websocket`).
  - Custom APIs: the `/api/{ns}/{db}/{*path}` route is default-on, and
    `DEFINE API` defaults to `PERMISSIONS FULL`, so an anonymous caller reaches
    a FULL handler. The HTTP response body is the handler's `body` value itself,
    not the `{status, body, headers}` object; a structured (object) body needs
    `MIDDLEWARE api::res::body("json")` (a string body is returned verbatim as
    `application/octet-stream`) or the layer answers 500; a method mismatch is
    404, not 405; a path is per-`(ns, db)` (404 under a different database)
    (`http`).
  - Access methods: a user-level JWT missing the `rl` (roles) claim silently
    authenticates as VIEWER rather than being rejected; the `AUTHENTICATE`
    clause runs on signup as well as signin; for user-level BEARER/JWT auth
    `$auth` is NONE and the role/claims live under `$session.tk` (`auth`).
  - A `UNIQUE` index on an array field indexes each ELEMENT independently — two
    rows collide when they share any single element, and the violation (an
    `InternalError`: "Database index `X` already contains VALUE, with record
    `RID`") names the one overlapping element, not the array (`indexes`).
  - Full-text BM25 uses a Robertson IDF clamped at 0, so a term present in a
    majority (> half) of documents scores exactly 0 for every matched row even
    though the rows still match and are returned (`search`).
  - `DEFINE EVENT`: `$event` is exactly `CREATE`/`UPDATE`/`DELETE`; `$before` is
    absent on CREATE and `$after` absent on DELETE; inside a nested `CREATE` in
    the event body a bare `$this` rebinds to the row being created, so the
    triggering document must be captured with `LET $doc = $this` (`$value`
    always tracks the triggering doc) (`events`).
  - Wire types: a bound `Uint8Array` (and a `<bytes>` cast) decodes back as an
    `ArrayBuffer`, not a `Uint8Array`; a `BigInt` beyond 2^53 roundtrips as a
    native `bigint` exactly, including as a record-id key part — no float
    precision loss (`surrealql-wire`).
  - Field-level `PERMISSIONS FOR select` denial drops the key entirely (absent,
    not null) from a record user's decoded document, including a computed
    `VALUE` field whose select predicate fails; root bypasses field permissions
    (`permissions`, `http`).
  - Record-user write denial is silent: a CREATE under `FOR create NONE` and an
    UPDATE/DELETE of a row failing the per-action `WHERE` both return an empty
    result with no error and no mutation — the same silent-filter pattern that
    role-denied data writes follow (`permissions`).
  - `DEFINE FUNCTION` / `DEFINE PARAM` permission denial is LOUD, not silent: a
    non-root caller hitting `PERMISSIONS NONE` / a false predicate gets "You
    don't have permission to run the fn::X function" / "view the $X parameter"
    over both the `run` RPC and `query`; root bypasses (`programmability`).
  - Search honours permissions: `@@` full-text and `<|K|>` vector KNN results
    are filtered to the rows/fields a record user may select. KNN filters AFTER
    selecting the K nearest with no backfill, so a `<|2|>` whose nearest point
    the user cannot see returns fewer than K rows — it never leaks the hidden
    row (`search`).
  - View read-only enforcement is permission-ordered: a direct write to a
    `DEFINE TABLE ... AS SELECT` view errors loudly with `TableIsView` only when
    permissions are bypassed (owner); for a record user the permission check
    runs first, so a denied write is silently filtered — no view-ness leak. A
    view's non-select permissions normalize to `NONE` (`views`).
  - `sequence::nextval` stays contiguous and duplicate-free across concurrent
    connections on one node — `BATCH` is a KV reservation size, not a
    per-connection stride (cross-node dedup is a separate TiKV concern)
    (`sequences`).
  - `ON DELETE UNSET` requires an `option<record<…>>` field and removes the key
    entirely (absent, not null); a non-optional reference field refuses the
    parent delete. In a non-transactional batch a rejected `ON DELETE REJECT`
    statement does not abort later statements (`references`).
  - `ACCESS … PURGE` returns a flat grant array while `REVOKE GRANT` nests one
    level deeper (`[[grant]]`); the default purge grace is 0s but eligibility is
    a strict `>` at whole-second granularity (`auth-refresh`).
  - Wire value types: closures are not representable — `RETURN |$x| $x` fails the
    whole response with "Closure values cannot be converted to public value"
    (`type::of` still reports `function`); a regex literal is a parse error, so
    it never reaches the encoder; `<set>` decodes as a native JS `Set`; a `<file>`
    value needs `--allow-experimental=files` and the `f"bucket:/key"` form (the
    `<file>"…"` cast fails); a bound record-id range reports `type::of` `record`,
    not `range` (`surrealql-wire`).
  - RPC surface: `insert_relation` returns an array even for a single edge
    (unlike `relate`, which returns one object); `reset` clears the `ns`/`db`
    selection as well as auth + params; `revoke` of an access-only token leaks
    the internal `refresh()` function name in its error; in `INFO … STRUCTURE`
    a per-field permission map omits the `delete` key while per-table maps carry
    all four (`websocket`).
  - HTTP: a CBOR-body `/rpc` request must set an explicit `Accept: application/cbor`
    (fetch's implicit `*/*` maps to JSON → 415); an empty `{}` export config
    exports everything (the default), not nothing; the anonymous `/metrics`
    scrape is filtered to public metric families (build/process gauges), with
    per-route/tenant families withheld (`http`).
  - Cross-transport error shapes diverge by design: GraphQL reports schema-layer
    failures as HTTP 200 with the error in `errors[]` (but missing ns/db as a
    transport-level 400, resolved from headers); a bad-credentials failure is a
    bare `text/plain` 401 on `/sql`, `/graphql`, and `/gql` even under
    `Accept: application/json`; and a WebSocket parse error fails the whole
    request as a top-level `-32000` while runtime errors arrive as per-statement
    `ERR` envelopes (`errors`).
  - Time-travel: `SELECT … VERSION` works on a versioned datastore
    (`memory?versioned=true`) without a changefeed; versioned reads resolve
    against commit timestamps, so a boundary-exact `time::now()` capture is racy
    (bracket writes with a small sleep). `VERSION` on a subquery source is
    rejected — it must be placed inside the subquery (`versioning`).
  - `FROM ONLY` collapses the result to a bare object/scalar across
    SELECT/CREATE/UPDATE/DELETE (and `EXPLAIN` returns a bare plan-object the
    same way); an `UPDATE` on a missing id returns `[]` only when the table
    already exists, otherwise it errors "table does not exist" (`surrealql`).
  - `CHANGEFEED … INCLUDE ORIGINAL`: an UPDATE entry carries the after-image
    under `current` plus an `update` array of REVERSE JSON-patch ops (applying
    them to `current` reconstructs the prior value); a DELETE carries the
    before-image verbatim under `delete.original`; a CREATE is still a plain
    `{ update: { …after } }` (`changefeeds`).
  - `search::offsets(n)` decodes as an object keyed by the indexed FIELD
    position (not the `@n@` matchref) — `"0"` for a single-field FULLTEXT index
    — each value an array of `{s, e}` char spans sorted by start (`search`).

## Scope grown so far / next

- [x] Auth: root/system users, roles, record access (signup/signin),
      token authenticate/invalidate, session expiry; BEARER access grants +
      `signin({access, key})`, external JWT access (roles→level mapping, the
      missing-`rl`→VIEWER default), and the `AUTHENTICATE` clause (`auth`)
- [x] Refresh tokens: `WITH REFRESH` definitions, rotation via
      `authenticate({access, refresh})`, single-use reuse detection, grant
      revocation and `DURATION FOR GRANT` expiry, `invalidate()` scope
- [x] Live queries: CRUD actions, cross-connection delivery, kill,
      permission-filtered delivery (CREATE, UPDATE, and DELETE arms),
      `WHERE`-filtered live streams, multi-subscriber
- [x] Sessions: use, set/unset, per-connection isolation, binding scope
- [x] Multiplexed sessions: newSession/forkSession/sessions/closeSession,
      per-session auth + ns/db + params on one WebSocket, live-query
      ownership, invalidate vs close
- [x] Transactions: implicit per-statement, BEGIN/COMMIT/CANCEL/THROW/RETURN
      response anatomy, SDK cross-call `beginTransaction()`, snapshot
      isolation, commit conflicts
- [x] Concurrent-writer conflict behavior over WebSocket: two connections,
      pipelined single connection, implicit table-creation races, `.retry()`
- [x] SurrealQL wire semantics: rich-type CBOR roundtrips, complex record
      ids, NONE/NULL, bindings vs LET, protected params, UTF-8/NUL, large
      responses
- [x] HTTP endpoints (`/sql`, `/key`, `/signin`, `/signup`, `/export`,
      `/import`, RPC-over-HTTP) against the same specs
- [x] GraphQL endpoint conformance: config gates, schema generation and
      invalidation, list/filter/aggregate/connection queries, mutations,
      permissions, route denial
- [x] ISO GQL dialect (default-on): `/gql`, RPC `gql`,
      `eval::gql`, route gating, parser limits; multi-hop MATCH with edge
      predicates, MATCH..SET/REMOVE/DELETE mutations, HTTP Accept negotiation,
      WebSocket-RPC `$vars` + txn interop (`gql`)
- [x] WebSocket JSON-RPC wire conformance (raw `json` subprotocol): connection
      RPCs (ping/version/signin/signup/invalidate/authenticate), CRUD verb
      envelope shapes, run/relate/info, session reauthentication + the expired-
      session per-method matrix, RPC capability gating, live/kill raw
      notification frames (`ws`, ported from `ws_integration.rs`)
- [x] Capability enforcement matrix: default / `--deny-all` / `--allow-all` /
      `--deny-scripting`, function and network allow/deny precedence
      ("deny wins on match"), and the guest-access matrix (`capabilities`,
      ported from `cli_integration.rs::test_capabilities`)
- [x] HTTP endpoint security cases: `/key` injection guard, RPC-over-HTTP
      session hijack/isolation, `--deny-http`/`--allow-http` + `--*-arbitrary-
      query` route matrices, `/signin` level inference, `--client-ip` modes,
      the readiness gate, `surreal-id`/identification headers, `/sync`
      (`http`, ported from `http_integration.rs`)
- [x] SDK wire subset ported from `api_integration`: changefeeds over RPC
      (`SHOW CHANGES` polling), `export()`/`import()` round-trips (SDK + raw
      HTTP, incl. hostile-identifier escaping), and query-result shapes —
      bindings, ORDER BY/START/LIMIT, record-id ranges, FETCH, DELETE ranges,
      decimal coercion, UPDATE CONTENT (`changefeeds`, `backup`, `surrealql`)
- [x] Schema-constraint enforcement: `DEFINE FIELD` `ASSERT`/`READONLY`/`VALUE`/
      `DEFAULT`, `DEFINE INDEX ... UNIQUE` (single/composite/array), and
      `DEFINE EVENT` triggers (`$before`/`$after`/`$event`) (`surrealql`,
      `indexes`, `events`)
- [x] Search: full-text (`DEFINE ANALYZER`, `SEARCH ... BM25` index, `@@`,
      `search::score`/`search::highlight`) and vector KNN (`HNSW` index,
      `<|K|>`, `vector::distance::*`) (`search`)
- [x] Graph traversal: multi-hop and reverse/bidirectional edge walks
      (`->e->n`, `<-e<-n`, `<->e<->n`) (`surrealql`)
- [x] `eval::surql` / `eval::gql` capability gate (`capabilities`) and the
      arbitrary-query subject gate on the RPC verbs, not just `/sql`
      (`websocket`)
- [x] Custom API endpoints: `DEFINE API` + the `/api/{ns}/{db}/{*path}` route —
      handler dispatch, body serialization middleware, cross-tenant isolation,
      and `--deny-http=api` route gating (`http`)
- [x] SDK transaction bound CRUD: `create`/`update`/`merge`/`delete`/`insert`/
      `relate`/`select` on a `beginTransaction()` handle stay isolated until
      commit and never auto-commit (`transactions`)
- [x] Permission enforcement through a real record session: field-level `FOR
      select` redaction (incl. computed `VALUE` fields), per-action write denial
      (`FOR create NONE`, `FOR update`/`delete WHERE`), and cross-transport
      parity — the same permissions redact identically over WS-RPC, `/sql`, and
      `/key` (`permissions`, `http`)
- [x] Programmability permissions: `DEFINE FUNCTION` and `DEFINE PARAM`
      `PERMISSIONS` enforced by auth level over both `run` and `query`, plus
      global-param cross-connection visibility (`programmability`)
- [x] Auth at ROOT and NAMESPACE level: system BEARER/JWT access, system users,
      level/role scoping, and signin level inference (`auth`)
- [x] Search permission enforcement: `@@` full-text and `<|K|>` vector KNN
      honour a record user's row and field permissions (`search`)
- [x] Data integrity: `DEFINE SEQUENCE` / `sequence::nextval` (contiguous,
      duplicate-free under concurrent connections), record references
      `ON DELETE` CASCADE / REJECT / UNSET (+ cascade live-notify, non-txn batch
      partials), and computed/materialized view read-only enforcement
      (`TableIsView`, permission-before-view ordering) (`sequences`,
      `references`, `views`)
- [x] `REMOVE TABLE` terminates live subscriptions on other connections (server
      emits `KILLED`) (`live`)
- [x] Grant/session lifecycle: `ACCESS ... PURGE EXPIRED/REVOKED` and system-user
      `DURATION FOR SESSION` / `FOR TOKEN` expiry (`auth-refresh`, `auth`)
- [x] Wire value types: ranges & record-id ranges, closures and regex
      (non-representable), sets, files, and the rich `<cast>` matrix
      (`surrealql-wire`)
- [x] RPC surface: `insert_relation`, `reset`, `revoke`, the `detach` guard, and
      `INFO … STRUCTURE` wire shapes (`websocket`)
- [x] HTTP: `/rpc` CBOR content negotiation, `export(options)` selective export,
      and the anonymous `/metrics` scrape (`http`)
- [x] Cross-transport error-shape consistency across WS-RPC, `/sql`, GraphQL,
      and `/gql` (`errors`)
- [x] Time-travel: `SELECT … VERSION` on a versioned datastore, including the
      subquery-source rejection (`versioning`)
- [x] Query surface: `FROM ONLY` + `SingleOnlyOutput`, CREATE/UPDATE/UPSERT
      id-existence semantics, query `TIMEOUT`, and `EXPLAIN` plan decode
      (`surrealql`)
- [x] `CHANGEFEED … INCLUDE ORIGINAL` before-image / reverse-patch feed shape
      (`changefeeds`)
- [x] `search::offsets` wire shape and bound-parameter `<|K|>` KNN (`search`)

### Rust → JS test migration

The `*-port.test.ts` files are ports of the wire-level Rust integration suites
(`ws_integration.rs`, `http_integration.rs`, `graphql_integration.rs`,
`gql_integration.rs`, the wire subset of `api_integration/`, and
`cli_integration.rs::test_capabilities`). `MIGRATION.md` holds the full
Rust→JS mapping, the per-file parity checklist (covered / keep-in-Rust /
deferred), and the Phase-3 deletion manifest. No Rust tests have been deleted
yet — the ports run alongside the Rust suite until the JS suite is green in CI
for 1–2 weeks. `pg_integration.rs` is deferred (no pg-wire in the server test
binary).
