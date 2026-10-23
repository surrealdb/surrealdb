# Read-only transactions with bounded staleness

Status: Part A and Part B are largely implemented (see "Implementation status"). The document has two parts that are
independent changes. Part A adds `READ ONLY [MAX STALENESS <duration> [STRICT]]`
to `BEGIN`, with a transport-level form for single statements. Part B
(`ISOLATION LEVEL`, the `SNAPSHOT` default, and removing per-request isolation
headers) is a separate change; Part A neither needs it nor is blocked by it.

Every path below is taken from `origin/main` at `31a22adfa`.

# Part A: read-only transactions with bounded staleness

## Implementation status

Implemented: the `BEGIN [TRANSACTION] READ ONLY [MAX STALENESS <duration or
param> [STRICT]]` syntax, its printing and lowering, `BeginOptions` through the
dispatch and executor, the `StaleReads` datastore seam
(`surrealdb/kvs/src/stale.rs`) with the operator maximum and the `allow`,
`deny` and `rate:<n>` fall-back policies
(`surrealdb/datastore/src/factory.rs`), the current-state default for backends
without a stale tier, and `read_only` pass-through in the PostgreSQL and MySQL
translators.

Also implemented, as the transport surface:

- **Snapshot description.** The `BEGIN` of a read-only block answers with
  `{ position, age, stale }` instead of `NONE`; the `begin` RPC answers with
  `{ id, snapshot, isolation }` when it carries options (`snapshot` for a
  read-only one). `position` is the backend's
  opaque token as bytes, or `NONE`.
- **Request form.** `QueryRequest::read_only` wraps a request's statements in
  the block (`BEGIN … COMMIT` results included, so the result indexes are those
  of the wrapped text). RPC `query` and `query_stream` take it as the third
  parameter, `{ read_only: { max_staleness, strict } }`; HTTP `/sql` takes the
  query-string parameters `read_only_max_staleness` and `read_only_strict`,
  which are removed from the variables.
- **`begin` RPC options.** The same options object, over WebSocket and the
  embedded and local engines; the SDK's `Begin` builder has `.read_only()`,
  `.max_staleness(Duration)` and `.strict()`, and the `Transaction` has
  `.snapshot()`. `COMMIT` of a read-only transaction releases it.

Part B is implemented too: `ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE }` parses
and prints, in either order with `READ ONLY`; `SNAPSHOT` is the default for
every transaction; the `IsolationLevels` backend handle
(`surrealdb/kvs/src/isolation.rs`) offers other levels, and a backend without
one refuses `SERIALIZABLE` naming the levels it offers; `Datastore::begin_transaction`
opens the transaction a `BEGIN` asks for, refusing `SERIALIZABLE` with a
`MAX STALENESS` above zero; the `begin` RPC takes `isolation` in its options
object and answers with an object whenever it carried options; the SDK's
`Begin` builder has `.isolation(...)`, and refuses a server that answers a
read-only or isolation request with a bare id; the PostgreSQL parser,
translator and connection layer map `ISOLATION LEVEL` as the table below
describes, refuse `READ COMMITTED` and `READ UNCOMMITTED`, and no longer ignore
the level; pgwire also honours `default_transaction_isolation` and refuses those
levels there. The memory engine (SurrealMX) publishes an
`IsolationLevels` handle that opens read-write transactions under serializable
snapshot isolation for `SERIALIZABLE`, and reports their plain reads as tracked;
SurrealKV and TiKV offer `SNAPSHOT` alone and refuse `SERIALIZABLE`,
since their commits validate only written keys and locked reads.

Not yet implemented: gRPC (its `begin` and `query` messages carry no options;
a gRPC `begin` with options is refused), commit positions, the read-only
snapshot lifetime cap, and `ReadSnapshotTooStale` as a distinct typed error
kind (it is a query error carrying the name in its message). Removing a
per-request isolation header from an engine that carries one is that engine's
change.

## Why

A transaction that only reads does not need the newest committed state. A
datastore that can serve an older, already-published snapshot can answer such a
transaction without any coordination on the read path: no fence, no leader
round trip, no lock. For point reads at very high rates that is the difference
between a read tier that scales horizontally and one that is bounded by its
coordinator.

The opt-in belongs in the language, on the statement that opens the transaction,
because it changes what the transaction means. A client that states "this
answer may be five seconds old" gets the same meaning on every datastore and
every transport. Routers that must act on the bound without parsing SurrealQL
read an equivalent transport field (see "Single statements and routers"), which
is defined as sugar for the language form.

## Current behaviour

- `BEGIN [TRANSACTION]` takes no options. `TopLevelExpr::Begin` is a unit
  variant (`surrealdb/expr/src/parser/query.rs`).
- A `BEGIN` block opens `TransactionType::Write` (`execute_begin_statement_impl`,
  `surrealdb/core/src/dbs/executor.rs`). A lone statement opens a read
  transaction when `TopLevelStatement::read_only()` holds.
- `BEGIN` is a `TopLevelExpr`, which the parser accepts only at the top level
  (`top_level_statement`, `surrealdb/parser-next/src/parse/stmt/mod.rs`). It
  cannot occur inside an `Expr`, so no stored definition (a function, event or
  API body) can contain it.
- The `begin` RPC takes no options and opens `TransactionType::Write`.
- The datastore opens transactions through
  `TransactionBuilder::new_transaction(write: TransactionType)`
  (`surrealdb/kvs/src/builder.rs`). There is no snapshot-age input.

## Syntax

```surql
BEGIN [ TRANSACTION ] [ READ ONLY [ MAX STALENESS <duration-or-param> [ STRICT ] ] ] ;
```

`COMMIT` and `CANCEL` are unchanged.

- `BEGIN;` and `BEGIN TRANSACTION;` keep their meaning: a read-write transaction
  reading the current state.
- `BEGIN READ ONLY;` is a read-only transaction that reads the current state.
  Any write errors. It is the standard `READ ONLY` mode.
- `BEGIN READ ONLY MAX STALENESS 5s;` is a read-only transaction whose snapshot
  may be up to five seconds older than the moment the transaction opens.
- `MAX STALENESS` is valid only after `READ ONLY`. `BEGIN MAX STALENESS 5s;` is
  a parse error that names the missing `READ ONLY`.
- `STRICT` changes what happens when the bound cannot be met (below).

The operand of `MAX STALENESS` is a duration literal or a parameter (`$s`). A
parameter is resolved when the transaction opens, from the variables bound to
the request, and must hold a duration; any other value is an error that names
the parameter. A parameter keeps the statement text, and so any cache keyed on
it, identical across different bounds.

`READ`, `MAX` and `STALENESS` are matched as contextual identifiers inside the
`BEGIN` statement, case-insensitively, and are not reserved. Existing queries
that use them as field, table or parameter names stay valid. `ONLY` and
`STRICT` are already keywords (`surrealdb/parser-next/src/kw.rs`).

### Bounds

- The minimum bound is `0s`: the current state. It is equivalent to `READ ONLY`
  without `MAX STALENESS`.
- A negative or non-duration operand is an error.
- The maximum is an operator setting on the datastore. A request above it fails
  when the transaction opens with an error that names the limit. It is not
  clamped: clamping would silently change the contract the client wrote down.
- A backend with a stale tier declares its snapshot publication interval as
  `min_staleness` (see "Datastore seam"). A bound below it is valid and is met
  only when a snapshot happens to be fresh enough; otherwise the unmet-bound
  rules apply. The declared values are what operators and clients consult to see
  which bounds are meaningful on a deployment.

## Semantics

### Snapshot

All statements in the transaction read one snapshot, chosen when the transaction
opens and fixed for its life. The snapshot's age at selection is at most the
bound. The bound is not re-evaluated while the transaction runs, so a long
transaction finishes reading data older than the bound.

The datastore, not the executor, measures age. The executor never compares a
snapshot timestamp with its own clock, so clock skew between nodes cannot turn
into a silent violation of the bound.

Clock functions (`time::now()` and the like) return the wall-clock time at the
moment of evaluation, not the snapshot's time. A stale transaction therefore
reads old data and a current clock.

### Pinned snapshots

A transaction holding an old snapshot prevents the datastore from reclaiming the
history that snapshot needs. Read-only transactions that opened with
`stale: true` are capped by an operator setting, the read-only snapshot
lifetime, measured from open. A transaction that outlives it fails on its next
statement with `ReadSnapshotExpired`, naming the cap, and is cancelled. The cap
is at most the maximum staleness bound plus the datastore's transaction timeout;
the ordinary transaction timeout applies as well and the earlier of the two
wins. Fresh read-only transactions are governed by the ordinary timeout alone.

### Writes

A `READ ONLY` transaction opens a read transaction at the datastore
(`TransactionType::Read`, `surrealdb/kvs/src/lib.rs`). Any write fails with
`Error::TransactionReadonly` (`surrealdb/kvs/src/err.rs`), surfaced to the client
as a read-only-transaction error rather than a storage error.

Two layers enforce this:

1. The syntactic predicate `TopLevelExpr::read_only`
   (`surrealdb/expr/src/parser/query.rs`) rejects statements known to write
   before they run.
2. The datastore rejects every write the predicate cannot see: a function that
   writes, an event, a sequence `nextval`, an API handler.

The predicate produces a better message. The datastore check is the guarantee.

### When the bound cannot be met

The datastore may hold no snapshot within the bound: it has just started, the
publisher is behind, or the bound is below the publication interval. `open`
reports this as a distinct outcome, `BoundUnmet`, carrying the bound and the age
of the newest available snapshot. What the datastore does next is the
**fall-back policy**, an operator setting:

| Policy | Without `STRICT` | With `STRICT` |
|---|---|---|
| `allow` (default) | Opens a fresh read in place; the metadata reports `stale: false`, age `0s`. | Fails with `ReadSnapshotTooStale`. |
| `deny` | Fails with `ReadSnapshotTooStale`. | Fails with `ReadSnapshotTooStale`. |
| `rate-limited(n/s)` | Opens a fresh read in place while the rate is within `n` per second, else fails as `deny`. | Fails with `ReadSnapshotTooStale`. |

`STRICT` is the client's refusal to be served fresh; `deny` is the operator's.
Under `deny` every read-only transaction behaves as if it were `STRICT`.

`ReadSnapshotTooStale` is a typed error carrying the bound and the newest
available age. A router that read the transport field (below) can recognise it
and redirect the request to a fresh tier, so a deployment can keep fresh reads
off the stale tier instead of serving them from it. A datastore with no stale
tier meets every bound with the current snapshot, so none of this applies there.

### What the transaction returns to the client

A **position** is an opaque, datastore-defined token naming a point in the
datastore's commit order. It is comparable only with tokens from the same
datastore, bounded in size, and free of namespace, database and credential
material. It is absent when the backend has no such notion.

Each response from a transaction opened with `READ ONLY` carries snapshot
metadata:

| Field | Meaning |
|---|---|
| `position` | The position of the snapshot the transaction reads. |
| `age` | The snapshot's age when the transaction opened. `0s` for a fresh read. |
| `stale` | `true` when the snapshot is older than the current state at open time; `false` after a fall-back or on a backend with no stale tier. |

A read-write transaction's commit result carries `position`: the position at
which its writes became visible. It is the same field, with the same bounds, and
makes read-your-writes across nodes possible. A later extension can take a
minimum position on `BEGIN` (for example `AFTER <position>`); that clause is out
of scope here, and the field shape above is what it will consume.

The metadata belongs to the transaction, so it is attached to the response, not
to individual statements. Clients that ignore it are unaffected.

### Interactions

| Construct | Behaviour in `READ ONLY` |
|---|---|
| `SELECT ... FOR UPDATE` and other locked reads | Error. A read transaction cannot register a lock: the locked-read call (`Transactable::getu`, `surrealdb/kvs/src/api.rs`) returns `TransactionReadonly`. |
| `SHOW CHANGES` | Allowed; it is classified read-only. It sees the changefeed as of the snapshot, so it can omit the newest changes by up to the bound. |
| `LIVE SELECT`, `KILL` | Error. Both write registration state. |
| `DEFINE`, `ALTER`, `REMOVE`, `REBUILD` | Error (write). |
| `USE`, `LET`, `RETURN`, `IF`, `FOR`, `OPTION` | As without the option. |
| Nested `BEGIN` | Error, as without the option (`FailCause::NestedBegin`, `executor.rs`). |
| Savepoints | The `BEGIN` grammar exposes none. A read-only transaction has no writes to roll back to. |
| `COMMIT` / `CANCEL` | Both release the snapshot. `COMMIT` of a read-only transaction commits nothing and cannot fail with a conflict. |
| A failing statement or `THROW` | As without the option: the transaction is cancelled and the remaining statements are not executed. |

### Authorization on a stale snapshot

Permissions, field permissions, users and access methods are data. Two reads
touch them, with two bounds:

- **Statement reads** (table and field permissions evaluated while the
  transaction runs) read the transaction's snapshot, so a statement never mixes
  a new permission with old rows. Their staleness is the transaction's bound
  *b*.
- **Authentication reads** (`signin`, token verification, session expiry) cannot
  be required to read the current state, because a datastore whose stale tier is
  stateless has no current state to read. The datastore declares an
  `auth_staleness` bound *a* in the handle below: `0s` where it can read the
  current state, a published interval where it cannot. A deployment may instead
  verify tokens without any datastore read; that is *a* equal to the token's own
  validity.

The revocation delay a client can observe is at most max(*b*, *a*). Both are
capped by the operator maximum, which is therefore a security-relevant
deployment setting and is stated in the operator documentation.

A stale transaction resolves catalog and index state at the version of its own
snapshot, and no cache entry populated from a stale snapshot may be published to
a cache shared with fresh transactions. How the existing datastore caches
(`surrealdb/datastore/src/cache/`) are keyed must be audited before
implementation; this document does not assume it.

## Single statements and routers

Single-statement transactions read the current state. A stale read of one
statement, and a router's view of the bound, use a transport field defined as
exact sugar for the language form:

- RPC `query` takes an optional third parameter, an options object holding
  `read_only: { max_staleness, strict }`. HTTP `/sql` takes the query-string
  parameters `read_only_max_staleness` (a duration) and `read_only_strict` (a
  boolean), which are not bound as variables.
- A request carrying the field behaves exactly as if its statements were
  wrapped as `BEGIN READ ONLY MAX STALENESS <d> [STRICT]; <statements> COMMIT;`.
  One snapshot serves every statement. The results are those of the wrapped
  text, the `BEGIN` and `COMMIT` results included. A request whose own text
  contains `BEGIN`, `COMMIT` or `CANCEL` is refused: the first such statement
  would end the wrapping block and let the statements after it run outside it,
  as writes. An options object without `max_staleness` asks for a bound of
  `0s`, the current state.
- The executor applies the field as the options of the request's implicit
  block, without rewriting the statement text. The text, and any compiled
  statement cache keyed on it, is therefore the same for every bound, which is
  the reason the duration is never spliced into the query.
- A router reads the field without parsing SurrealQL, and can redirect on
  `ReadSnapshotTooStale`.
- Semantics live in the language: the field adds no behaviour the `BEGIN` form
  lacks, and the metadata and errors are the same.

The edge entry (`surrealdb/core/src/kvs/ds/edge.rs`) serves single statements in
transactions of their own and refuses `BEGIN` with `not_served("BEGIN")`; that
refusal stands. It honours the field by opening one read-only transaction with
the field's options for the request, and refuses a request that carries the
field and has more than one statement, because separate transactions would give
the statements separate snapshots and break the sugar's meaning.

## Parser

`origin/main` has one SurrealQL parser. The tag-based syntax tree in
`surrealdb/parser-next` is lowered into the runtime AST by
`surrealdb/expr/src/parser`. `surrealdb/core/src/syn/mod.rs` only re-exports the
parser with settings derived from datastore capabilities.

### Tree and runtime types

- `surrealdb/parser-next/src/ast.rs`: `Tag::Begin` is a leaf. Nodes are 20-byte
  `(tag, op, a, b, start, end)` records, so the options fit without a new node
  kind: `op` holds flag bits (`READ ONLY`, `STRICT`), and `a` holds the
  `Tag::Duration` or `Tag::Param` leaf for `MAX STALENESS`, or `NONE`. The bit
  layout is documented on the `Begin` variant, as other tags do.
- `surrealdb/expr/src/parser/query.rs`: `TopLevelExpr::Begin` becomes
  `Begin(BeginOptions)`:

  ```rust
  pub struct BeginOptions {
      pub read_only: Option<ReadOnly>,
  }
  pub struct ReadOnly {
      pub max_staleness: Option<Staleness>,
      pub strict: bool,
  }
  pub enum Staleness {
      Duration(Duration),
      Param(String),
  }
  ```

  `BeginOptions::default()` renders as `BEGIN`, so existing printed queries are
  unchanged. The options type derives `Eq` because `Dispatch` does.

### Sites that change

Parsing and lowering:

- `surrealdb/parser-next/src/parse/stmt/misc.rs`, `transaction`: parse the
  clauses after the optional `TRANSACTION`, for `BEGIN` only. `CANCEL` and
  `COMMIT` keep the leaf form. Match the contextual words with the `word`
  helper that `explain_stmt` uses for `ANALYZE` and `FORMAT`, guarded by
  `self.tok.kw == Kw::None`. Reject duplicates and `MAX STALENESS` without
  `READ ONLY` with errors that name the clause.
- `surrealdb/expr/src/parser/mod.rs`, `top`: the `Tag::Begin` arm builds the
  options.
- `surrealdb/expr/src/parser/entry.rs`: the `dedicated` tag list already holds
  `Tag::Begin`; no change.
- `surrealdb/expr/src/parser/query.rs`: the credential-redaction arm
  (`Self::Begin | Self::Cancel | Self::Commit => {}`), `read_only`, the `ToSql`
  arm that prints `BEGIN`, and `into_execution_units`, which groups a `BEGIN`
  block and treats the new variant the same way. The printer's output must
  re-parse to the same value.
- The statement-start recovery lists in `surrealdb/parser-next/src/parse/mod.rs`
  and `surrealdb/parser-next/src/parse/stmt/write.rs` name only the keyword and
  do not change.

Execution and planning (every match on `TopLevelExpr::Begin`):

- `surrealdb/core/src/dbs/top_level.rs`: `dispatch` (`Dispatch::Begin` carries
  the options), `request_kind`, `statement_type`.
- `surrealdb/core/src/dbs/executor.rs`: `run_surface_statement` (the misplaced
  control arm), the `RequestEntry::Begin` construction and its consumers, the
  `StatementRole::Begin` arms, `role_agrees_with_dispatch`,
  `execute_begin_statement_impl`.
- `surrealdb/core/src/exec/ir/build/logical.rs`: `request_kind` and the
  `Begin | Commit | Cancel` invariant arm.
- `surrealdb/core/src/exec/ir/cache/names.rs` and
  `surrealdb/core/src/exec/ir/cache/slots.rs`: the `Begin | Cancel | Commit`
  no-op arms.
- `surrealdb/core/src/kvs/ds/edge.rs`: `admit` keeps refusing `BEGIN`.
- `surrealdb/pgwire/src/conn.rs`: the `has_begin` check.
- Tests that construct or match the variant: `surrealdb/core/src/sql_to_sql_test.rs`,
  `surrealdb/core/src/fmt_roundtrip_test.rs`,
  `surrealdb/core/src/dbs/top_level_test.rs`,
  `surrealdb/core/src/dbs/read_only_test.rs`.

`Dispatch` is "what a driver acts on before a statement is converted", so it
carries the options; the drivers read them where they open the transaction.

## Executor

All in `surrealdb/core/src/dbs/executor.rs`.

- `execute_begin_statement_impl` opens
  `kvs.transaction(TransactionType::Write)`. It takes the options from the
  dispatch, resolves a parameter operand, and calls the option-aware open
  (below). The existing failure path (no transaction could be created:
  fast-forward to `COMMIT`/`CANCEL` and report each statement as not executed)
  handles `ReadSnapshotTooStale` and the maximum-bound error without a new path.
- A request carrying the transport field takes the same open path with the
  field's options as the options of its implicit block.
- The request grouper (`RequestGrouper`, `StatementRole`) is unaffected: `Begin`
  is `Begin`.
- The opened transaction's snapshot metadata is recorded on the executor's
  result set so the transports can attach it. A committed read-write
  transaction records its commit position the same way.
- Observer events: `StatementType::Begin` stays the label. The snapshot age,
  whether the read fell back, and the fall-back policy outcome are attributes on
  the `BEGIN` statement event, so the stale tier's hit rate is measurable.
- A stale transaction past the read-only snapshot lifetime fails its next
  statement with `ReadSnapshotExpired`.
- Broker preparation: a read-only transaction has no live-query notifications to
  send and prepares its broker as the single-statement read path does
  (`matches!(transaction_type, TransactionType::Write)`).

## Datastore seam

Optional backend capabilities are published through
`TransactionBuilder::extension(TypeId)` (`surrealdb/kvs/src/builder.rs`), under a
shared `TypeId` when several backends can offer the same capability, as
`DestroyRangeHandle` is. Stale reads follow that convention instead of widening
the trait every backend implements:

```rust
pub struct ReadSnapshotRequest {
    pub max_staleness: Duration,
}

pub struct SnapshotInfo {
    pub position: Option<SnapshotPosition>,
    pub age: Duration,
    pub stale: bool,
}

pub enum ReadOutcome {
    Opened(Box<dyn Transactable>, bool, SnapshotInfo),
    BoundUnmet { newest_age: Duration },
}

/// Published under a shared `TypeId` by backends with a stale tier.
pub trait StaleReads: Send + Sync {
    /// The snapshot publication interval: the smallest bound usually met.
    fn min_staleness(&self) -> Duration;
    /// The largest bound served from a published snapshot.
    fn max_staleness(&self) -> Duration;
    /// The bound the datastore applies to authentication reads.
    fn auth_staleness(&self) -> Duration;
    fn open(&self, request: ReadSnapshotRequest) -> BoxFut<'_, Result<ReadOutcome>>;
}
```

The handle is published only while the backend can honour it, as the existing
`extension` contract says; `None` is the caller's signal to use the default.
`open` never opens a fresh read itself: it reports `BoundUnmet`, and the
datastore applies the fall-back policy, so a deployment can route the decision.

### Default for ordinary engines

A backend that publishes no handle gets this, applied by `TransactionFactory`
(`surrealdb/datastore/src/factory.rs`), not by the backend:

- `READ ONLY` opens `new_transaction(TransactionType::Read)`.
- Any `MAX STALENESS` is met by that current snapshot: the factory reports
  `SnapshotInfo { position: None, age: 0s, stale: false }`, the fall-back policy
  is never consulted, and `STRICT` never fails.
- `auth_staleness` is `0s`.

A single-node engine needs no code for the syntax to be meaningful: a query
behaves identically everywhere apart from how old the data may be.

### Backends with a stale tier

`open` picks the newest snapshot within the bound, or reports `BoundUnmet`. The
datastore applies the operator's maximum bound before calling `open`, the
fall-back policy after a `BoundUnmet`, and the snapshot lifetime cap for the
life of the transaction.

`Transactable` (`surrealdb/kvs/src/api.rs`) is unchanged: the transaction a
stale snapshot returns is an ordinary read transaction, which is why executors,
indexes and planner paths work on it without modification. Writes reach the
existing `TransactionReadonly` checks.

This is separate from `Transactable::snapshot` and `view_capabilities`
(`surrealdb/kvs/src/snapshot.rs`), which hand an owned read view of one
transaction to other threads. That view answers *how* a transaction's reads are
shared; `MAX STALENESS` answers *which committed state* the transaction reads.
It is also separate from a `VERSION` read at a stated timestamp: a version read
names the data; a staleness bound lets the backend choose it and report what it
chose.

### Commit positions

`Transaction::commit` (`surrealdb/datastore/src/tx.rs`) returns the commit's
position when the backend has one (`Option<SnapshotPosition>`), so the
transports can attach it to the commit result. A backend with no notion of
position returns `None`. This does not depend on the stale tier: any backend may
provide it.

### Factory, datastore and transaction

- `TransactionFactory::transaction` gains a sibling that takes the read snapshot
  request; `Datastore::transaction` (`surrealdb/core/src/kvs/ds.rs`) exposes the
  matching entry point and holds the operator settings (maximum bound,
  fall-back policy, snapshot lifetime cap).
- `Transaction` carries the `SnapshotInfo` it was opened with.
- A stale transaction does not register as the writer of anything and does not
  advance sequences or the changefeed watermark; read transactions already
  satisfy this.

## Transport surface

`BEGIN` options travel in the query text, so every transport that carries
SurrealQL carries them. The transport field above is the router-readable form.
HTTP RPC rejects `begin` and keeps doing so.

### Manual transactions over RPC

The `begin` RPC takes no options and opens `TransactionType::Write`. The sites:
`surrealdb/rpc/src/method.rs` (`Method::Begin`),
`surrealdb/core/src/rpc/protocol.rs` (dispatch at `Method::Begin`, default
`begin`), `surrealdb/server/src/rpc/websocket.rs` (`begin`, which opens the
transaction after enforcing the per-connection cap),
`surrealdb/server/src/rpc/grpc.rs` (`begin`, `begin_transaction`, the method-name
table), `surrealdb/server/src/rpc/http.rs`, `surrealdb/engine-api/src/lib.rs`
(`Command::Begin`, `fn begin`), `surrealdb/engine-local/src/engine.rs`,
`surrealdb/src/engine/remote/mod.rs` and `surrealdb/src/engine/remote/grpc.rs`,
and the SDK builder `surrealdb/src/method/begin.rs` (`Surreal::begin`).

Add an optional options object to `begin` carrying `read_only` and its
`max_staleness` and `strict`. Absent, it behaves as today. The SDK's `Begin`
builder gains `.read_only()`, `.max_staleness(Duration)` and `.strict()`. The
gRPC `begin_transaction` call, which currently sends an empty parameter array,
sends them.

`begin` reserves a transaction slot before opening anything and releases it on
every failure path. Options are validated before the slot is taken; an open that
fails with `ReadSnapshotTooStale` releases the slot like any other failure. A
read-only transaction holds a snapshot and counts toward the cap.

### Returning positions and metadata

- The `begin` reply is the transaction id. With options it returns the id and
  the snapshot metadata.
- Query responses carry it in the result of the read-only `BEGIN`, which
  answers `{ position, age, stale }` where a plain `BEGIN` answers `NONE`. A
  separate response-level field would need a new field on every result, and a
  result value is visible to every transport and SDK unchanged. A request that
  committed writes will carry `commit_position` the same way once a backend
  provides one.
- The streaming protocol (`QueryStreamFrame` in `surrealdb/rpc/src/stream.rs`,
  `surrealdb/embedded/src/stream.rs`, the gRPC frames in
  `surrealdb/server/src/rpc/grpc.rs`) carries them on the frame that closes the
  `BEGIN` block, or on `End` when there is none. They are covered by
  `QUERY_STREAM_VERSION`; a client that does not know the fields ignores them. A
  stream that fails to open the transaction reports the error on the terminal
  frame like every other failure.
- HTTP `/sql` returns them in the response object beside the existing
  statistics. No header carries them.

### Security review (`SECURITY_GUIDE.md`)

The change touches sections 3, 6, 14 and 15 and Appendix B.

- Parser (15): the new productions are bounded by the parser's existing depth
  and token limits and allocate nothing proportional to input beyond one
  duration or parameter name. They evaluate nothing at parse time; a parameter
  is resolved at open, not parse. Round-trip tests cover the faithful-AST
  invariant.
- RPC (6): the open-transaction cap and its release-on-failure rule are
  unchanged and tested on the failing-open path; open transactions, read-only
  ones included, are cancelled on disconnect; the metadata and position fields
  are bounded. The transport field is validated before any transaction opens.
- Execution context (3): namespace, database and session are resolved when the
  transaction opens and do not change; the snapshot is chosen at the same point
  and is fixed.
- Storage (14): read-after-write inside the transaction is trivial for a
  read-only transaction. Cross-snapshot cache reuse is the new hazard, handled
  under "Authorization on a stale snapshot". The lifetime cap bounds snapshot
  pinning.
- Authorization: the revocation delay is max(*b*, *a*), explicit, documented
  and capped by the operator maximum.
- Wire protocols (Appendix B): see the next section; an unsupported mode is a
  syntax error in the dialect, never a fall-through.

## SQL front-ends

PostgreSQL and MySQL statements are translated to SurrealQL text and parsed by
the SurrealQL parser. Their handling of `READ ONLY` today:

| Site | Behaviour |
|---|---|
| `surrealdb/pgsql/src/parser/stmt/tx.rs` | `BEGIN`/`START TRANSACTION` parse `READ ONLY` and `READ WRITE` into `TransactionStmt::Begin { read_only, span }`. |
| `surrealdb/pgsql/src/translate/stmt.rs`, `translate_transaction` | Matches `Begin { .. }` and always emits `BEGIN TRANSACTION;`. **`read_only` is dropped.** |
| `surrealdb/mysql/src/parser/stmt/tx.rs` | `START TRANSACTION READ ONLY` / `READ WRITE` parse into `StartTransaction { read_only, span }`; `BEGIN [WORK]` into `Begin(span)`. |
| `surrealdb/mysql/src/translate/stmt.rs`, `translate_transaction` | Matches `Begin(_) \| StartTransaction { .. }` and always emits `BEGIN TRANSACTION;`. **`read_only` is dropped.** |
| `surrealdb/pgwire/src/conn.rs`, `transaction_control` | Intercepts a standalone `BEGIN`/`START TRANSACTION`, computes `read_only` and opens `TransactionType::Read` for it. |

The two translators are where `READ ONLY` is accepted and not honoured. Mapping:

| Source | Emitted |
|---|---|
| MySQL `START TRANSACTION READ ONLY` | `BEGIN TRANSACTION READ ONLY;` |
| MySQL `START TRANSACTION [READ WRITE]`, `BEGIN [WORK]` | `BEGIN TRANSACTION;` |
| PostgreSQL `BEGIN READ ONLY` | `BEGIN TRANSACTION READ ONLY;` |
| PostgreSQL `BEGIN READ WRITE`, plain `BEGIN` | `BEGIN TRANSACTION;` |

Rules:

- Neither dialect has a spelling for `MAX STALENESS`. A stale read is reachable
  from them only by switching the dialect to SurrealQL, or through the transport
  field on the request that carries the statement.
- The translators emit the clause in the order the SurrealQL printer uses, and
  the emitted text must re-parse. Tests assert the exact emitted text for every
  row.
- `SECURITY_GUIDE.md` Appendix B requires fail-closed behaviour: a mode a
  dialect cannot express is a syntax error in the dialect and never falls back
  to another dialect.
- Isolation modes in these dialects are handled by Part B.

## Test plan

### Language tests (`language-tests/CLAUDE.md`)

New files under `language-tests/tests/language/statements/transaction/`. Each
`.surql` file runs under three planner strategies, so each covers both engines.

- `read_only_rejects_writes`: `CREATE`, `UPDATE`, `DELETE`, `DEFINE` and a
  function-borne write inside `BEGIN READ ONLY`, each erroring and leaving no
  data. Include a write the syntactic predicate cannot see, to pin that the
  datastore check is the guarantee.
- `read_only_reads_one_snapshot`.
- `read_only_max_staleness_default_engine`: `MAX STALENESS 5s`, `STRICT` and a
  `$s` parameter operand succeed on the in-memory datastore and return current
  data, pinning the trivially-met default.
- `read_only_max_staleness_param_errors`: a parameter that is not a duration, an
  unbound parameter.
- `read_only_for_update_errors`, `read_only_live_select_errors`,
  `read_only_show_changes_works`, `read_only_clock_is_wall_time`.
- `begin_options_errors`: `MAX STALENESS` without `READ ONLY`, duplicated
  clauses, a negative duration, a bound above the configured maximum (set
  through `[env]`).
- `begin_words_stay_usable`: tables, fields and parameters named `read`, `max`
  and `staleness` still parse.
- Each new test is shown to fail with its check reverted.

The stale path needs a backend that publishes snapshots, which the language-test
harness cannot supply. Those tests (a snapshot within the bound, `stale: true`,
`BoundUnmet` under each fall-back policy and with `STRICT`, the maximum-bound
error, the snapshot lifetime cap, `auth_staleness`) are Rust tests against a
test backend that publishes `StaleReads` with a controllable snapshot age, in
`surrealdb/core/src/kvs/tests/`. Commit positions are tested against a test
backend that returns one.

### Parser tests (`surrealdb/parser-next/CLAUDE.md`)

The parser is pinned by a golden corpus and invariant harnesses, not snapshot
files. Add every valid combination and error form to
`surrealdb/parser-next/tests/corpus/`, run
`cargo run --locked --release -p surrealdb-expr --example golden`, review the
digests and `-- --bless`. Blessing records the error wording, which is a product
contract, so the naming question below must be settled first. The round-trip,
streaming and depth invariants run over the same corpus (`cargo make ci-parser`).
Core-side printing round trips go in `sql_to_sql_test.rs` and
`fmt_roundtrip_test.rs`.

### Transport and SQL front-end tests

- The transport field: RPC and HTTP requests with and without it, the equivalence
  with the wrapped `BEGIN` form (same results, same errors, same metadata), a
  request whose own text contains `BEGIN`, the edge entry's single-statement
  acceptance and multi-statement refusal.
- `surrealdb/pgsql/tests/stmt_tests.rs`, `surrealdb/mysql/tests/parser_tests.rs`,
  `translation_tests.rs`, `printer_tests.rs`, unit tests for `transaction_control`
  in `surrealdb/pgwire/src/conn.rs`, and `surrealdb/pgwire/tests/e2e.rs` and
  `surrealdb/mywire/tests/e2e.rs`: a `READ ONLY` transaction rejects a write end
  to end.
- RPC: `begin` with and without options; the transaction cap with read-only
  transactions; slot release when the open fails; the metadata and position
  fields present and bounded (`surrealdb/server/src/rpc/websocket.rs` tests,
  `surrealdb/server/tests/`, `surrealdb/embedded/src/stream.rs`).

### Upgrade tests (`CLAUDE.md`)

None required.

- No storage key, index layout or revisioned catalog struct changes, so
  `revision.lock` and `surrealdb/core/src/kvs/compat/` are unaffected.
- `BEGIN` cannot appear in a stored definition: it is a top-level-only
  statement (see "Current behaviour"), so the options are never persisted and
  are not a durable SurrealQL construct.
- Positions are transport-only. A backend that persists one anywhere a later
  release reads it must add the upgrade pair then.

## Open questions

Ranked by how much the answer changes the design.

1. **Revocation delay.** Statement reads evaluate permissions from the
   snapshot, for consistency. Is a revocation delay of max(*b*, *a*) acceptable
   for every deployment, or must permission and access definitions always be
   read at the authentication bound, giving up snapshot consistency between data
   and permissions?
2. **Naming.** `MAX STALENESS` (two words) against a single `STALENESS`, and a
   trailing `STRICT` against an explicit `ON UNMET ERROR`. Either is a
   syntax-only change, but it must be settled before the parser corpus is
   blessed. The proposal is `MAX STALENESS` with a trailing `STRICT`.
3. **The maximum bound and the lifetime cap.** What defaults for the operator
   maximum and the read-only snapshot lifetime, and is exceeding the maximum an
   error (proposed) or a clamp?
4. **Fall-back policy defaults.** `allow` is the proposed default so that
   clients stay correct without configuration. A deployment whose fresh tier is
   expensive would set `deny` or `rate-limited`; should the default differ when
   a stale tier is published?
5. **Read-your-writes.** The position fields are fixed here. Is the next step an
   `AFTER <position>` clause on `BEGIN`, so that position is a first-class
   input?
6. **Metadata placement.** Per-response statistics (proposed) or a dedicated
   result for `BEGIN`? Statistics leave result arrays unchanged, which avoids
   breaking positional indexing in existing clients.
7. **Transport field spelling.** `read_only` object on RPC and two query-string
   parameters on HTTP, against a single `isolation`-style options object shared
   with Part B.

# Part B: isolation level (independent change)

Part B does not affect Part A and can land before, after or without it. It
adds `ISOLATION LEVEL` to `BEGIN`, fixes `SNAPSHOT` as the language default, and
removes per-request isolation headers that datastores may carry.

## Syntax and default

```surql
BEGIN [ TRANSACTION ] [ ISOLATION LEVEL { SNAPSHOT | SERIALIZABLE } ] [ READ ONLY ... ] ;
```

The isolation and `READ ONLY` clauses may appear in either order, at most once
each. `ISOLATION`, `LEVEL`, `SNAPSHOT` and `SERIALIZABLE` are contextual
identifiers, not reserved.

`SNAPSHOT` is the default for every transaction: a `BEGIN` with no isolation
clause, and each single statement outside `BEGIN`. The default is a property of
the language, so it is the same on every datastore.

`SNAPSHOT` conflicts on a transaction's writes only; `SERIALIZABLE` also
conflicts on everything the transaction read. A datastore that cannot provide
`SERIALIZABLE` rejects it with an error that names the levels it supports and
never silently downgrades it. `ISOLATION LEVEL SERIALIZABLE` combined with
`READ ONLY MAX STALENESS` is rejected when the transaction opens: a serializable
read must not come from a snapshot older than the data it must conflict with.

## Per-request headers

A datastore that selects isolation with a transport-level header removes it:
`BEGIN ... ISOLATION LEVEL SERIALIZABLE` expresses the same choice. The level is
an argument to the open call, not ambient request state, so it reaches every
transaction the `BEGIN` opens regardless of which task opens it. Single
statements are always `SNAPSHOT`; serializable work uses `BEGIN`, or the `begin`
RPC with its isolation option.

## Seam

```rust
pub struct TransactionOptions {
    pub write: TransactionType,
    pub isolation: Option<Isolation>,
}

/// Published under a shared `TypeId` by backends that offer more than one level.
pub trait IsolationLevels: Send + Sync {
    fn supported(&self) -> &[Isolation];
    fn open(&self, options: TransactionOptions)
        -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>>;
}
```

A backend that publishes no handle gets `SNAPSHOT`, which every backend's
ordinary transaction provides; `SERIALIZABLE` is rejected, because the factory
cannot know the backend provides it. If Part A has landed, `open` also carries
the read snapshot request; the two handles then merge into one options type.

## Sites

The same parser, executor and RPC sites as Part A, with an `isolation` field
added to `BeginOptions`, a flag in the `Begin` node's `op` bits, an `isolation`
member of the `begin` RPC options object and the SDK `Begin` builder
(`.isolation(...)`), and a `TransactionOptions` entry point on
`TransactionFactory` and `Datastore`. `execute_begin_statement_impl` passes the
level to the open call.

## SQL front-ends

PostgreSQL `ISOLATION LEVEL` is accepted and mapped, so that no isolation level
is accepted and ignored. Today the PostgreSQL parser
(`surrealdb/pgsql/src/parser/stmt/tx.rs`) rejects `ISOLATION` with "unsupported
transaction isolation level", while `transaction_control` in
`surrealdb/pgwire/src/conn.rs` accepts `ISOLATION`, `LEVEL` and `SERIALIZABLE`
among a modifier whitelist and ignores them.

| Source | Emitted clause |
|---|---|
| `ISOLATION LEVEL SERIALIZABLE` | `ISOLATION LEVEL SERIALIZABLE` |
| `ISOLATION LEVEL REPEATABLE READ` | `ISOLATION LEVEL SNAPSHOT` (PostgreSQL's own `REPEATABLE READ` is snapshot isolation) |
| `ISOLATION LEVEL READ COMMITTED`, `READ UNCOMMITTED` | refused: a transaction at these levels sees commits made while it runs, which a snapshot never does, so `SNAPSHOT` would be a different behaviour, not a stronger one |
| `DEFERRABLE`, `NOT DEFERRABLE` | accepted only with `SERIALIZABLE READ ONLY`, where it has no observable effect here; otherwise a syntax error |

The PostgreSQL parser's `Begin` node gains an isolation field and keeps the
"unsupported" error for anything outside the table. `transaction_control`
stops accepting modifiers it cannot honour: it either parses with the PostgreSQL
parser and opens the transaction from the parsed statement, or parses the same
modes itself and opens with `TransactionOptions`. Tests assert the emitted text
for every row and that an unlisted mode is rejected rather than dropped.

The PostgreSQL translator renders `SAVEPOINT` and `RELEASE SAVEPOINT` as SQL
comments (`-- SAVEPOINT {name};`), another place a statement is accepted and not
run; it is independent of both parts.

## Tests

Language tests under `language-tests/tests/language/statements/transaction/`:
`SERIALIZABLE` accepted or rejected per engine, single statements read at
`SNAPSHOT`, `SERIALIZABLE` with `MAX STALENESS` rejected, and the words staying
usable as identifiers. Parser corpus entries as in Part A. A Rust test against a
backend that publishes `IsolationLevels` for the open path. pgwire and mywire
end-to-end tests for the mapping. No upgrade test is needed, for the reasons in
Part A.

## Open questions

1. **Is `SNAPSHOT`-only acceptable for single statements?** If a
   single-statement serializable form is wanted, the transport field of Part A
   can carry it.
2. **Drivers that name `READ COMMITTED`.** Some ORMs and drivers send it
   explicitly and now receive a clear error until they are configured to
   `REPEATABLE READ` or `SERIALIZABLE`. The same rule applies to the
   `default_transaction_isolation` and `transaction_isolation` settings, which
   refuse those levels, report `repeatable read` by default, and are honoured by
   a `BEGIN` that names no level.
