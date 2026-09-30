# Upgrading SurrealDB

This document records per-release operational notes for SurrealDB
administrators: wire-format rotations, removed features, configuration
changes, and other impacts that aren't apparent from version numbers
alone. Read this before deploying a new release onto an existing
database.

For the release **process** (how releases are cut and shipped), see
[`RELEASING.md`](./RELEASING.md). For end-user release notes, see
<https://surrealdb.com/releases>.

## v3.3.1 — vector indexes that lost records sharing a vector

**Impact**:

- An HNSW or DiskANN index in which more than nine records share one identical
  vector is missing those records from KNN results, and may keep returning
  fewer than `k` rows after such records are deleted. This release stops the
  loss; an index already affected keeps it until `REBUILD INDEX`.

### Vector indexes that lost records sharing a vector

HNSW and DiskANN keep, for every distinct vector, the set of records holding
it. Once that set grew past nine records, every later addition to it and every
removal from it was applied in memory but never written back, on every release
since 3.0.0. The index therefore silently omitted the tenth and later records
holding that vector from KNN results. It also kept records deleted from such a
set: they are dropped when the result is built, so a query could return fewer
than `k` rows, and the vector itself stayed in the graph after its last record
was gone. This release writes every change.

**What to do**: rebuild each HNSW and DiskANN index whose table may hold more
than nine records with an identical value in the indexed field — duplicated
embeddings, placeholder or default vectors — with `REBUILD INDEX`. An index
where every vector is shared by at most nine records was never affected.

## v3.3.0 — datastore version stamp, startup data migrations, the sequence keyspace move, module re-keying, rollbacks reported as `cancelled`, ANN pending entries at a new revision, and export/import contract changes

**Impact**:

- The first v3.3.0+ node to start records data migrations it has applied; a
  build that lacks one of them refuses to start.
- Sequence definitions gain a second home, and a sequence created mid-rollout
  is not visible to older nodes.
- A silo module defined by v3.2.4 or earlier is unreachable under v3.3.0+ until
  migration 2 runs, and once it has run, older nodes stop resolving that
  module. `mod::` modules are unaffected.
- Dashboards that count transactions with
  `surrealdb_transaction_total{outcome="success"}` will read lower after the
  upgrade. Nothing about transaction behaviour changes — only how a rollback is
  labelled.
- An HNSW or DiskANN index that was written by v3.1.0-beta.1 through v3.2.4
  while any indexed record's id held a number inside an array or object —
  `chunk:[42, 'intro']`, `doc:{tenant: 1}` — keeps a stale doc-ID mapping for
  those records. They are missing from kNN results, with no error; the query
  simply returns fewer rows than the `k` it asked for. The record-keyed pending
  queue this comes from arrives in v3.1.0-beta.1, so anything older is
  unaffected; DiskANN itself only exists from v3.1.0-beta.3. Run
  `REBUILD INDEX <name> ON <table>` once per affected index.
- Two records whose ids differ only in the numeric variant of a nested value —
  `c:[1]` and `c:[1dec]`, `c:{k: 1}` and `c:{k: 1f}` — share one forward doc-ID
  mapping in any index built before this release, so one of the two is absent
  from that index's results. An index built by v3.2.4 or earlier keeps its own
  per-index mapping and stays that way until `REBUILD INDEX`: installing this
  release changes nothing for it, and nodes of either version read it alike.
  A rebuild separates the two in a full-text index only. A plain index, and an
  HNSW or DiskANN index, still treat them as one record however they were
  built — their own entries key the id with the numeric variant dropped — so
  one of the two is missing from their results, and a kNN query can return the
  other twice. A unique index is unaffected: its entries are keyed by the
  indexed values alone.
- In a mixed-version cluster, a node on an older release fails kNN queries
  against an HNSW or DiskANN index once an upgraded node has written to it, and
  also fails a write to any record that index already has an update queued for,
  and its own background compaction of that queue. Upgrade every node, or stop
  both writing to and querying those indexes from the older ones until you
  have — routing only reads away is not enough.
- An index whose build is still running while a node on an older release keeps
  serving writes can finish wrong for records whose ids hold a number inside an
  array or object. A build records the writes that land while it scans, under a
  key naming the record; this release spells that name so it addresses one
  record, and an older node spells it the older way. A build re-derives those
  keys from the recorded writes when its scan starts — which covers a build
  carried across a single-node upgrade — so what it cannot account for is what
  an older node writes after that. For such a record the index can hold a wrong
  entry, not only miss one: a `COUNT` index can count it twice or not at all,
  full-text relevance statistics drift, and a `UNIQUE` build can fail, reporting
  the record as a duplicate of itself. Let builds finish before a rolling
  upgrade, or run `REBUILD INDEX <name> ON <table>` for any that were in flight
  once every node runs this release; a rebuild started while an older node still
  serves writes is exposed to the same thing.
- A store written by a v3.3.0 pre-release can carry index damage no stable
  release leaves behind, wherever an indexed record's id holds a number inside
  an array or object: an HNSW or DiskANN index the pre-release built can map
  such a record to a spelling no record is stored under, and a full-text, HNSW
  or DiskANN index can give one doc-ID to two records whose ids differ only in
  that number's variant. Once every node runs this release, run
  `REBUILD INDEX <name> ON <table>` once per such index. That repairs the
  mapping; of such a pair, only a full-text index comes out separated, as the
  note on those records says.
- `surreal export --versions` now fails instead of writing a dump. A backup
  script pinned to it exits non-zero and produces no file.
- `POST /import` answers `422` where it answered `200` for an import that only
  partly applied. A client that checked only the status was previously told
  every failure was a success.
- A dump's `DEFINE ACCESS` statements are refused on import where the access has
  a signing key, because a dump carries the redaction placeholder in place of
  one. A restore that reported success now reports one failure per such access,
  and the accesses are not created.
- An export now carries `DEFINE DATABASE OVERWRITE` for a database that has a
  `COMMENT`, `CHANGEFEED` or `STRICT` clause of its own, which rewrites those
  clauses on the target when the dump is restored.
- Restoring a dump written by an **older** release can now exceed
  `SURREAL_TRANSACTION_MAX_WRITE_KEYS`, where that guard is set. The dump is
  unchanged; what it costs to apply is not.

### Why an older node cannot read or write an ANN index an upgraded node has written

The pending queue an ANN index writes through carries the record each entry
belongs to, added at revision 2 of `HnswRecordPendingUpdate` and
`DiskAnnRecordPendingUpdate`. Without it the record id had to be recovered by
decoding the queue's key, which spells it with the codec used for indexed
values — that codec omits a number's kind, so an id holding a number inside an
array or object came back as a spelling no record is stored under, and the kNN
hit was dropped after consuming a slot in the result (surrealdb/surrealdb#7437).

A kNN query reads the whole uncompacted queue, so it decodes every entry there,
and a write to an already-queued record decodes that record's entry before
folding the new vectors into it. An older node cannot decode a revision-2 entry
and fails, rather than skipping it, in either case — and in the compaction that
drains the queue. The window opens on the first write an upgraded node makes to the
index and closes when every node is upgraded; it does not depend on the ids
involved. Compaction does not end it: an upgraded node's compaction drains the
revision-2 entries, so an older node may succeed for a while, and the next write
from an upgraded node re-arms it. Expect the failures to come and go rather than
to persist, and do not read a quiet interval as the problem having passed.

Nothing durable is harmed: the entries are the queue's own, the upgraded nodes
read and compact them normally, and an older node recovers as soon as it is
upgraded.

### Why an index written before the upgrade needs rebuilding

The doc-ID mapping those records were given points at a key spelled the way the
queue could express it, which for an id holding a number is not the key the
record is stored under. Nothing about installing this release replaces it: an
index keeps the doc-ID layout it was built with, and one built before
v3.3.0-beta.1 keeps reading and writing its own, per-index mapping.

`REBUILD INDEX` is what moves it. The rebuilt index derives its mapping from the
records themselves and joins the table's shared space, whose forward lookup
rejects an entry the reverse mapping does not name — so the missing rows come
back. There is no automatic repair; an index left un-rebuilt keeps returning
short.

A rebuild also separates two ids differing only in a nested number's kind, which
the per-index layout cannot: the shared space keys them injectively. That is the
one repair unavailable on the 3.2 line, where no such space exists.

### Startup now applies data migrations

A v3.3.0+ node stamps the datastore with its full semantic version at `/!vs`,
appends a version-history entry at `/!vh`, and records each data migration it
applies at `/!mg{id}`. Migration selection reads that ledger, not the stamp: a
migration runs when its ledger entry is absent and its declared version is at or
below the running build.

**Binary downgrade is one-way once a migration has been applied.** A build whose
registry does not contain a migration the ledger records exits at startup with
`The data stored on disk was migrated by a newer version of SurrealDB … and
cannot be read by this one`. The `MajorVersion` guard does not catch this, for
the same reason it does not catch the v3.1.0 rotation: the migration happens
inside the existing major version.

If you need the option to roll back, take and verify a logical backup
(`surreal export`) **before** running v3.3.0+ against live data.

### Sequence definitions moved out of the table band

`DEFINE SEQUENCE` definitions were stored at `/*{ns}*{db}*sq{name}`, which is
the same byte position a table name occupies. A sequence named `foo` therefore
shared its key with the keyspace root of a table named `sqfoo`, and listing
sequences on a database containing any table whose name begins with `sq` read
that table's rows and failed — taking `INFO FOR DB`, `surreal export`, and
`REMOVE DATABASE`/`REMOVE NAMESPACE` with it.

Definitions now live at `/*{ns}*{db}!sd{name}`. Migration 1 **copies** each
definition to the new location and leaves the original in place, so a node still
on the previous release keeps resolving its existing sequences throughout a
rolling upgrade.

What still works during a mixed-version rollout:

- Existing sequences, on nodes of either version. `sequence::nextval` continues
  from the pre-upgrade watermark; allocator state is not touched by the
  migration.
- Every other statement.

What doesn't:

- **A sequence created after the rollout begins is visible only to v3.3.0+
  nodes.** They write the definition to `!sd` alone; an older node resolves `*sq`
  and will not find it. Avoid `DEFINE SEQUENCE` until every node is upgraded, or
  re-run it afterwards.
- Rolling back to a pre-v3.3.0 build after any v3.3.0+ node has started, per the
  downgrade note above.

The retained `*sq` keys are inert under v3.3.0+ — nothing reads them — and a
later release will delete them once no supported upgrade path starts below
v3.3.0.

### Time-travel reads of sequences predate the move

On a versioned backend (`?versioned=true`), `INFO FOR DB VERSION <t>` and
`sequence::nextval` resolved at a timestamp *before* the migration committed
return no sequences: the `!sd` keys do not exist at those timestamps, and the
read path does not consult the retained `*sq` keys. Sequences at the present
timestamp are unaffected, as is every other catalog type.

### If a migration cannot finish

Migrations run inside the server's startup budget
(`--startup-operation-timeout`, default 60s). Migration 1's cost is proportional
to the number of tables whose names begin with `sq`, not to the number of
sequences, so it is fast on ordinary databases. Migration 3 reads every table's
doc-ID mappings, which exist only in a store written by a v3.3.0 pre-release;
on one of those its cost is proportional to the number of records any
full-text, HNSW or DiskANN index has mapped, at one round trip per mapping it
copies. A restart skips what an earlier attempt copied at the cost of a batched
read, so it does make progress, but each attempt still pays for the rest. If a
start times out during migration, raise that flag rather than restarting into
the same timeout.

### Why a silo module written by an older release cannot be reached

A module definition is stored at `/*{ns}*{db}!md{name}`, where `{name}` is not
part of the value but derived from it. `REMOVE`, `ALTER`, `DEFINE … OVERWRITE`
and a call to one of the module's exports all re-derive that name and read the
one key it spells.

The silo derivation gained a `::` before its version segment in v3.3.0, so
releases up to and including v3.2.4 wrote

```
silo::acme::widgets<1.2.3>
```

where v3.3.0+ derives

```
silo::acme::widgets::<1.2.3>
```

Nothing reported the mismatch. `INFO FOR DB` lists modules by scanning the band
and recomputing each name from the decoded value, so the definition rendered
exactly as it should while `REMOVE MODULE` on the name `INFO` had just printed
answered `The module '…' does not exist`. `DEFINE MODULE … OVERWRITE` then wrote
a *second* row at the derived key: `INFO FOR DB STRUCTURE` showed the module
twice, and `INFO FOR DB` still showed it once, because both rows recompute to
the same map key.

### Migration 2 moves the definition

Migration 2 walks the module band of every database and moves each row whose key
disagrees with the name its own value derives. Values are rewritten unchanged,
so permissions, comments and the `unsigned` flag carry across untouched.

Where both rows already exist — the store of an operator who worked around the
break with `DEFINE MODULE … OVERWRITE` — the addressable row wins and the
unreachable one is dropped, leaving one definition where there were two.

Unlike migration 1, this one **moves** rather than copies. The two keys hold the
same module, and the listing paths scan the band rather than addressing keys, so
leaving both behind would make every consumer of the module list report the
module twice. That is the state the migration exists to end.

What still works during a mixed-version rollout:

- Every `mod::` module, on nodes of either version. `mod::{name}` has never
  changed, and the migration does not touch those rows.
- Every other statement.

What doesn't:

- **A silo module stops resolving on nodes older than v3.3.0 once the migration
  runs.** No arrangement of the band avoids this: the two builds parse and derive
  different names for the same package, so no single key satisfies both. Upgrade
  every node before relying on a silo module, or re-run `DEFINE MODULE` once the
  rollout is complete.
- Rolling back to a pre-v3.3.0 build after any v3.3.0+ node has started, per the
  v3.3.0 downgrade note below.

### Time-travel reads of a silo module predate the move

On a versioned backend (`?versioned=true`), a silo module *call* resolved at a
timestamp before the migration committed fails to find the module: the derived
key does not exist at those timestamps. `INFO FOR DB VERSION <t>` still lists
the module, because listing scans the band and recomputes each name from the
value it finds. Silo modules at the present timestamp are unaffected, as is
every `mod::` module at any timestamp.

The migration's cost is proportional to the number of module definitions a
database holds, so it is negligible on any ordinary store.

### A rolled-back transaction is reported as `cancelled`

`outcome` on the `surrealdb.transaction` family previously took `success` for a
transaction that committed *and* for one that rolled back, because both report
the same result to the metrics layer. The two are now distinguished: `success`
means committed, `cancelled` means rolled back, and `error` stays reserved for a
commit or cancel that itself failed.

Rollbacks are routine — read-only work and internal probes end that way — so the
shift is not small on a busy node. To keep counting every transaction that
finished, admit both values:

```
surrealdb_transaction_total{outcome=~"success|cancelled"}
```

To count only work that reached durability, which is what the label now means,
keep filtering on `outcome="success"`. See the metric catalogue in
[`OBSERVABILITY.md`](./OBSERVABILITY.md) for the full attribute set.


### `--versions` is refused rather than ignored

A dump carries the current state of each record and has no grammar for a
record's history, so a versioned export was never possible. The flag was
accepted and then ignored: `surreal export --versions` wrote an ordinary dump
and reported success, and so did `.versions(true)` on the SDK builder and the
equivalent gRPC field.

All three now fail before any part of the response is sent, as a validation
error naming the option. There is no deprecation window, because the previous
behaviour was to silently answer a different question from the one asked.

**What to do**: drop `--versions` from backup scripts. The dump it produced was
already the one you get without it.

### `POST /import` reports a partial import

An import is not transactional: each statement commits on its own and execution
carries on past a failure. The endpoint previously returned `200` regardless,
so a dump that half-applied was indistinguishable from one that applied whole.

It now answers `422` when at least one statement did not apply, with the
failures in the body, in the format the request negotiated —
`application/octet-stream` has no structure to carry them in, so it gets the
messages as UTF-8 text, one per line. A `2xx` continues to mean every statement
applied. A request rejected *before* execution keeps its own `4xx` and leaves
no state behind.

The same distinction reaches gRPC as `failed_precondition`, and the embedded
engine — the WASM and Node SDKs — now returns the first per-statement failure
instead of resolving successfully.

**What to do**: a client that branches on the status needs no change to keep
working, but one that treated `200` as "restored whole" was wrong before and is
now told so. `surreal start --import-file` logs the failures and still starts;
it does not fail the boot.

### A dump's access definitions no longer restore

An export renders `DEFINE ACCESS` with the redaction placeholder in place of the
signing key, as every surface that renders an access does. That text is
executable SurrealQL, so replaying it defined an access whose secret was the
placeholder itself — a string published in the dump, in `INFO FOR DB` and in
this repository, which anyone can mint tokens with.

`DEFINE ACCESS` now refuses a definition naming the placeholder as its key, as a
validation error naming the access. Restoring a dump therefore reports one
failure for each access that had a key, and the database comes back without
them. An access verified by a JWKS URL carries no key and is unaffected.

**What to do**: re-create the affected accesses after the restore, from wherever
the real keys are kept — no dump has ever carried them. Where a dump is the only
record that an access existed, read its name and clauses out of the dump and
supply the key from your own store.

### An export can rewrite the target database's own clauses

An export carries a `DEFINE DATABASE OVERWRITE` line only when the database
holds a `COMMENT`, `CHANGEFEED` or `STRICT` clause of its own, and that line
names `$session.db` rather than a literal, so a restore lands in whichever
database the operator selected. Restoring such a dump therefore replaces those
three clauses on the target.

The statement authorises at namespace level, so a principal scoped to a single
database (`DEFINE USER … ON DATABASE`) cannot replay it even against their own
database, and the import is not transactional, so what they are left with is a
restore that mostly happened.

**What to do**: restore as a namespace- or root-level principal, or take the
dump from a database that carries none of those three clauses.

### An old dump can now overrun the write-key guard

`SURREAL_TRANSACTION_MAX_WRITE_KEYS` defaults to `0` (disabled), so this affects
deployments that set it.

An export groups records into `INSERT` statements sized so that each one stays
within that guard on the way back in, and the estimate is written into the dump
as the grouping itself. Before this release, an import wrote no reference keys
at all — a `REFERENCE` field cost nothing — so dumps were grouped as though
references were free. An import now writes them, and the exporter accounts for
them when sizing new dumps.

A dump written by an older release still carries the old grouping. A table with
`TYPE array<record<tag>> REFERENCE` holding ~20 ids was grouped at roughly
10,000 records per `INSERT`; applying that statement now writes ~420,000 keys
in one transaction.

**What to do**: where the guard is set, re-take backups on this release rather
than restoring pre-upgrade dumps, or raise the guard for the duration of a
restore from an older dump. The hazard is in the old dump rather than in
anything visible on the new node.

## v3.1.0 — rev-2 on-disk wire format for `Value`, `Array`, `Object`, `Set`

**Impact**: irreversible on-disk format rotation for every row that
contains a `Value`, `Array`, `Object`, or `Set`.

**Important**: once a v3.1.0+ binary writes a row of any of these types,
the bytes are in the new `revision(2, optimised)` format. A pre-v3.1.0
binary cannot read them. **Binary downgrade is one-way** for any
database the new build has touched, even with no other schema change.
The runtime `MajorVersion` guard
([`surrealdb/core/src/kvs/ds.rs::check_version`](../surrealdb/core/src/kvs/ds.rs))
does **not** catch this — the rotation happens inside the existing
major version, so an older binary opening a v3.1.0-written database
will see the expected `MajorVersion` and then fail at the per-row
`deserialize_revisioned` step when it encounters the unrecognised
rev-2 envelope.

What still works:

- Reading existing rev-1 rows under v3.1.0+ (forward migration is
  fully supported; the multi-revision walker handles both encodings
  transparently).
- All SurrealQL semantics — no query-level behaviour change.
- Replication and failover between v3.1.0+ nodes.

What doesn't:

- Restoring a backup taken from v3.1.0+ onto a pre-v3.1.0 binary.
- Failover from v3.1.0+ data to a pre-v3.1.0 node in a mixed cluster.
- Re-mounting a v3.1.0+ on-disk store with a pre-v3.1.0 build.

If you need the option to roll back, take and verify a logical backup
(`surreal export`) **before** running v3.1.0+ against the live data —
that backup is plain SurrealQL and can be replayed onto any binary
that supports its statements.

See the rationale block at the top of the `Value` enum in
[`surrealdb/core/src/val/mod.rs`](../surrealdb/core/src/val/mod.rs) for
the implementation-side explanation, and `doc/UPGRADING.md` will gain
a new section per future release that introduces another operational
impact of this kind.
