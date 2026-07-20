# JS conformance suite

Server-behavior conformance tests driven through the JavaScript SDK. This is
**not** an SDK test suite: the SDK is the driver, the server is the subject.
It covers connection-level behavior that `.surql` language tests cannot
express — authentication and refresh-token lifecycle, live queries, session
semantics (including multiplexed sessions), transactions and concurrent-writer
conflicts, wire-level value mapping, and the HTTP, GraphQL, and ISO GQL
endpoints (driven with raw `fetch` where the SDK has no surface for them).

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

## Scope grown so far / next

- [x] Auth: root/system users, roles, record access (signup/signin),
      token authenticate/invalidate, session expiry
- [x] Refresh tokens: `WITH REFRESH` definitions, rotation via
      `authenticate({access, refresh})`, single-use reuse detection, grant
      revocation and `DURATION FOR GRANT` expiry, `invalidate()` scope
- [x] Live queries: CRUD actions, cross-connection delivery, kill,
      permission-filtered delivery, multi-subscriber
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
- [ ] Access-grant purge (`ACCESS ... PURGE REVOKED` — note it applies an
      implicit grace window; pass an explicit `FOR <duration>` when testing)

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
