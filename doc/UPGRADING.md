# Upgrading SurrealDB

This document records per-release operational notes for SurrealDB
administrators: wire-format rotations, removed features, configuration
changes, and other impacts that aren't apparent from version numbers
alone. Read this before deploying a new release onto an existing
database.

For the release **process** (how releases are cut and shipped), see
[`RELEASING.md`](./RELEASING.md). For end-user release notes, see
<https://surrealdb.com/releases>.

## v4.0.0 — the record format switch, layout generations, ledger classes and the rollback check

A store records a **feature level**: the newest release whose on-disk
encodings the store may use. A store that records none is at **Legacy**, where
every record and every catalog entry is written in the `revision` encoding that
all 3.x releases read. Above Legacy, records are written in the stored-value
record format and catalog entries in the stored grammar. Which encoding a write
uses depends only on the level the writing node holds. Every row and every
entry carries enough in its first bytes for a reader to tell which encoding it
is in, and a store may hold both side by side.

**Impact**:

- A store on an engine with a single writer (memory, RocksDB, SurrealKV) that
  is at Legacy is raised to release 4.0 when this release starts. From then
  on, records and catalog entries are written in the new formats, and a 3.x
  release can no longer open the store.
- A store on an engine that does not report a single writer (TiKV, for one) or
  on IndxDB stays at Legacy until an operator raises it. A new store of any
  kind starts at this build's release.
- A store raised to 4.0 or above is refused at startup by 3.3, and cannot go
  back to 3.0 through 3.2.
- A store that holds a ledger entry this release does not know opens
  read-only or is refused, according to the entry's class.
- Every node records the release it runs beside its heartbeat, and a raise is
  refused while a live node runs an older release.

### The switch

The feature level decides the encoding of everything a node writes. A node
checks it on every write, from the level it holds in memory, and that level
follows the store's record.

- **Single-writer engines raise themselves.** The engine reports that no other
  process can write to the store, so there is no older node to wait for. At
  startup `check_version` raises a store at Legacy to release 4.0, not to this
  build's release, so the store can still be rolled back to any release of the
  4.x line, and logs the release. The raise commits together with the ledger
  entry described below. A store already above Legacy is not touched, and a
  second start opens no write transaction for it.
- **Other engines and IndxDB wait for an operator.** More than one process,
  possibly of different releases, can write to such a store, and a browser
  store may be open in several tabs. The store stays at Legacy, and every
  record keeps being written in `revision`, until an operator calls
  `Datastore::raise_feature_level`. The raise is refused with
  `FeatureLevelNodeBehind` while a node whose heartbeat is inside the liveness
  window records an older release, or none. The check guards against a partial
  rollout and is not a fence: a node that joins after the raise reads the level
  when it starts and writes no more than its own release reads. There is no
  SurrealQL syntax for the raise.
- **A pin raises to a release.** The `feature_level` configuration key
  (`SURREAL_FEATURE_LEVEL`), or `Builder::with_feature_level`, names a release
  (`4.0`, or `current`). A new store is created at it, and an existing store is
  raised to at least it when the datastore starts. A pin never lowers a level.
  A pin whose raise is refused because of an older node logs a warning, and the
  store keeps its level.
- **`Datastore::lower_feature_level` lowers the level** to a release at or
  above 4.0. It is refused while the store records that something in it uses a
  newer release's features, and that record only rises. A store cannot be
  lowered to Legacy.

### Holding a store at Legacy

`SURREAL_FEATURE_LEVEL=legacy`, or `Builder::with_legacy_feature_level`, keeps
a store that records no level at Legacy. Nothing raises it: neither the
single-writer raise nor a pin. The store gets no records in the new format, no
layout descriptors and no ledger entry for the format, so a 3.3.x build that
carries the readers for ledger ids 4 and 6 can still open it. Stock 3.3 is
refused by those two ids, which every 4.0 startup records, whatever the hold.

The hold applies only to a store at Legacy when it is opened. A store the build
creates is created at this build's release, and a store that already records a
level keeps it, each with one warning that the hold did not apply. The hold
never lowers a level. It is one setting with the pin: the last call on the
builder wins, and the builder's value beats the configuration key.

The `legacy` value widens what `SURREAL_FEATURE_LEVEL` accepts. The server has
no command-line flag for it.

### The ledger, its classes and read-only mode

The migration ledger (`/!mg`) lists the migrations and feature entries a store
has recorded. An entry's **class** lives in its key, in the top two bits of the
32-bit id, as the feature flags of ext4 do:

| Class | Id bits | A release that does not know the entry |
|---|---|---|
| Compatible | `01` | opens the store as usual, and logs the id and name at info |
| Read-only compatible | `10` | opens the store **read-only** |
| Incompatible | `00` | refuses the store at startup |
| Reserved | `11` | refuses the store at startup |

Only entries the running release has no migration for are judged. Class `00`
is the class of every id a release before the classes existed minted, so an
earlier release already treats an unknown one as it treats an incompatible
entry. A startup migration that a later 4.x release adds takes class
Compatible.

The record format entry (id 7, Incompatible) commits in the same transaction as
the first bytes that need it: the first record in the new format, the first
catalog entry in the stored grammar, the first layout descriptor, and the
transaction that raises the level. A store that holds bytes in the new formats
therefore holds the entry, and a release that does not know it is refused
before it reads those bytes. The entry is never run at startup and is never
part of a new store's baseline.

**Read-only mode** is set at startup, before any maintenance task starts, when
the ledger holds an unknown read-only compatible entry. It lasts for the life of
the process.

- Refused with `StoreReadOnly`: every write transaction (every statement that
  writes, `LIVE SELECT` included), every maintenance job, and the creation and
  refresh of durable RPC sessions.
- Still written: this node's own membership row and its release record, so the
  store keeps seeing the node as live, and the deletion of a durable session
  when its session is torn down, so that a torn-down session cannot be revived.
- Not started: every maintenance task except the node membership refresh. A
  session that expired is still refused when it is loaded.
- Skipped by the server, with one warning each: the configured import and the
  root credential initialisation. With no root user created, a root sign-in
  fails, so the store never opens without authentication.
- Reads are unaffected, and the storage version stamp is not advanced.

### Layout generations

Every table defined above Legacy has a **layout generation**: a descriptor
(`!lg{generation}` under the table's key range) that every record in the new
format names in its header. A descriptor is immutable. Only the DDL statement
that mints its generation writes it, in that statement's transaction. It names
its table and carries the release a reader needs.

A table's generations form a ring of the numbers 1 to 65535; 0 means "no
descriptor" and needs none. The table records its oldest generation, its
newest, and its floor: the oldest generation any of its rows is expected to
carry, or 0 when that is unknown (a table from 3.x or created at Legacy, and a
view). A table defined above Legacy with no rows starts with its floor at its
first generation. The floor guides planning only; every row is still checked as
it is read. Each mint takes the number after the newest, wrapping from 65535 to
1. When that number is the table's oldest generation every number is in use:
the DDL statement is refused with `GenerationRingFull`, naming the table and
its oldest generation, and writes nothing; the transaction that attempted it
commits nothing. A DDL statement that mints nothing still succeeds on a full
ring, and every row stays readable. The newest number minted for a table name
survives `REMOVE TABLE`, so a table defined again under the name continues the
numbering. A mint that finds a descriptor already stored at its number is
refused with `GenerationOccupied`.

A reader resolves a row's generation against the descriptors of the row's table
at its own snapshot. A generation outside the table's ring, or inside it with no
descriptor, reloads the table's descriptors once and is then refused with a
typed error, as is a header naming a number above 65535. A descriptor an earlier
release cannot read costs only the rows of that generation, and the rest of the
table stays readable. A table whose current generation is unreadable refuses
writes, and the next DDL statement on it mints a new generation. A table whose
ring record is damaged refuses every row in the new format, every write and
every mint; its rows in the `revision` encoding and of generation 0 still read.
Every 4.x node writes a table that has a generation in the new record format,
whatever level that node's cell reports, for every record the new format can
hold: an aggregated view row with statistics, and an edge in an earlier edge
format, are still written in the `revision` encoding.

A descriptor holds one of two layouts. The **document** layout stores a row as
a stored-value object. The **packed** layout stores a row as fixed-width slots
and variable slices, one column per declared field, so a reader can load a
declared field of a row without decoding the rest of it. A `SCHEMAFULL` table
that is not a view, not `DROP` and not a lightweight relation mints a new
generation at every DDL statement after which the layout derived from its field
definitions differs from its newest descriptor; a statement that changes no
declared field, kind or optionality (an index, an event, a permission, a
comment) mints nothing. Every other table derives the document layout. The
derived descriptor is a function of the table's definition and field
definitions alone, so two nodes of one release derive the same bytes from the
same catalog. A table whose field definitions this release cannot read keeps its
newest readable descriptor instead of minting.

A row of a table whose newest descriptor is packed is written in the packed
layout when every value fits its column, and as a document at the same
generation otherwise (for example a value of another type under an `OPTION
IMPORT`, a key no field declares, or a datetime that is a leap second). Rows of
every earlier generation stay as they were written. Every 4.0 reader reads all
of them in one table: `revision` rows, documents at generation 0, documents at
a generation and packed rows at a generation, and answers the same value
whichever it meets.

Each stamp of a table whose derived layout differs from its newest descriptor
takes one number of the table's ring, so a table defined one field per
statement takes one per field, and a statement that writes several of a table's
definitions (a relation's `in` and `out`, a view rebuild) can take more than
one. The descriptors a reader loads grow with them. A number is read through the descriptor that currently holds it:
once the ring wraps and a number is minted again, a row still stamped with its
earlier use would be read through the new descriptor, which for a packed row
gives wrong values with no error. Whatever moves a table's floor (a rewrite of
its rows) must therefore first make sure that no row, and no transaction that
could still write one, carries a number before the new floor.

A table created above Legacy records the fields that every row it will hold is
known to conform to (`!fc`). The record covers the fields defined in the
transaction that created the table, while the table holds no record; a field
defined by a later transaction is not added to it.
The record never changes how a row is read or written. Any later change to a
field definition of the table removes it, and so does an `OPTION IMPORT` write of
a row stamped with a generation: two such imports into one table at once conflict
over the removal, and the client retries the one that fails.

### Going back to an earlier release

- **3.3** reads the ledger. A store raised to the new format holds the
  Incompatible entry 7, which 3.3 does not know, so 3.3 refuses to start with
  `MigratedBeyondStorageVersion` naming the entry, before it reads any record.
  Stock 3.3 also refuses every 4.0 store, whatever its level, because the 4.0
  startup migrations record ids 4 and 6.
- **3.0 through 3.2** have no ledger and check only the storage version stamp
  (`!v`). A store carries stamp 4 from the moment it is created above Legacy,
  and from the transaction that raises its level or commits its first
  stored-format bytes, so these releases refuse a raised store at startup with
  their out-of-date error. A store that has not been raised keeps stamp 3, which
  they open as they always did; they already could not open a store that any 3.3
  or later build had migrated. A 4.x release opens stamp 3 and stamp 4. Stamp 4
  is not lowered by `lower_feature_level`, which never goes below 4.0. A value in
  the new format is never misread by them either: no `revision` value starts with
  `0xFF` (a record) or `0xFE` (a catalog entry in the stored grammar).
- **Within the 4.x line** a store can be rolled back from one release to the
  previous one. The earlier release opens the store unless the ledger refuses
  it, as above. A catalog entry whose read floor is above the earlier release
  is refused when read, and a field, index, event or foreign table entry among
  them makes its table refuse writes there. A layout descriptor above its
  release costs only that generation's rows. A table's generation ring and its
  generation mark are readable by every 4.x release: their format never
  requires a later one. A conformance record (`!fc`) above the earlier release
  is read as "no field known to conform", and the earlier release's next field
  change or `OPTION IMPORT` write to the table removes it.
- **Version history.** An earlier release that opens a store of a later release
  appends a note to the version history and leaves the stamp alone. When the
  later release runs again, it rechecks the state the earlier release does not
  maintain, for every check introduced since the lowest release that wrote to
  the store, and appends an entry that retires the notes. `version_history`
  lists the notes and the entries.

### `rollback_check`

`Datastore::rollback_check(target)` reports what a build of an earlier 4.x
`target` release would refuse in the store. It is a Rust API, not SurrealQL. It
is read-only: it writes nothing and takes no part in opening the store. It
reports:

- the ledger entries the target lacks, each with its class;
- the layout descriptors inside their table's generation ring whose release is
  above the target, and whether each is the table's newest generation;
- the catalog entries whose read floor is above the target, with their scope,
  and whether the entry makes its table refuse writes (a conformance record
  above the target means the target treats the table as having no conforming
  field; it never blocks writes);
- the count of entries and keys that do not parse, which are not named because
  a malformed value has no trustworthy name. A damaged generation mark or ring
  record, and a layout key outside its table's ring, are counted here: no
  release writes a ring or mark an earlier 4.x release refuses, and a damaged
  mark blocks the next mint of a table of that name that has no ring;
- the downward notes of the version history;
- whether the store's `in_use` level is above the target, which makes
  `lower_feature_level` refuse that release.

Its `verdict()` follows the ledger alone, which is what the target applies when
it opens the store: refused for an Incompatible or reserved entry, read-only
for a read-only compatible entry, open otherwise. The other findings list what
the target refuses after it has opened the store, one statement or row at a
time. Run it before lowering the level or starting the earlier release.

### A `LET` inside a cancelled transaction is undone

Parameters set with `LET` inside a transaction are discarded when the
transaction is cancelled or fails, as its writes are. After
`LET $b = 0; BEGIN; LET $b = 2; CANCEL;`, `$b` reads `0`, and a name the
transaction bound for the first time holds nothing again. A `LET` inside a
transaction that commits keeps its value. A client that read a parameter bound
inside a cancelled transaction must bind it again outside the transaction.

## v3.4.0 — export/import contract changes, vector indexes that lost records sharing a vector, a doc-ID → elements mapping on vector indexes, JACCARD/PEARSON as distances, named record-id components, and pre-filtered KNN choosing its strategy by cost

**Impact**:

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
- An HNSW or DiskANN index in which more than nine records share one identical
  vector is missing those records from KNN results, and may keep returning
  fewer than `k` rows after such records are deleted. This release stops the
  loss; an index already affected keeps it until `REBUILD INDEX`.
- HNSW and DiskANN indexes defined or rebuilt on this release keep a doc-ID →
  elements mapping, which lets a highly selective filtered KNN score its
  candidates from the index instead of reading every matching record. An index
  defined before the upgrade has no mapping and keeps answering from the
  records, correctly but more slowly, until `REBUILD INDEX`.
- `vector::similarity::jaccard` is now the weighted Jaccard similarity of vector
  search, `Σ min(aᵢ, bᵢ) / Σ max(aᵢ, bᵢ)`, compared position by position; it
  was a Jaccard over the *sets of values* in the two arrays. Its results change,
  and vectors of different dimensions are now refused, as by every other
  `vector::similarity::*` function.
- KNN under `JACCARD` and `PEARSON` now ranks by a distance, `1 − similarity`
  and `1 − r`, so the most similar vectors come first; it ranked the least
  similar first. `vector::distance::knn()` values change accordingly, and an
  HNSW index defined with either metric needs `REBUILD INDEX`.
- A value containing a deeply nested `GeometryCollection` that was stored by an
  earlier release may now return `The value is nested too deeply` when read. It
  can be repaired in place by overwriting the field; see below.
- A containment condition (`CONTAINS`, `INSIDE`, `CONTAINSANY`, `CONTAINSALL`,
  `ANYINSIDE`, `ALLINSIDE`) on a field indexed as `field.*` is answered by a
  table scan unless the query itself writes the flatten (`field.* CONTAINS v`).
  Results are now complete; on a large table the plain spelling is slower. See
  below.
- B-tree index plans follow what each index has recorded storing, not a
  field's declared type. An index built by an earlier release recorded nothing,
  so every query over it plans as if its fields could hold an array, typed or
  not: `ORDER BY` sorts instead of reading the index in order, a scan lists each
  record once rather than stopping at a `LIMIT`, a range open at its upper end
  reads the table, `count()` over an indexed condition reads the matching
  records, and a KNN query checks a range condition during the search. Rows are
  unchanged. `REBUILD INDEX`, once every node runs this release, restores the
  index plans for a field that never held an array. See below.
- A table whose `id` field names the components of its array ids, such as
  `DEFINE FIELD id ON reading TYPE [city: string, time: datetime]`, cannot be
  read by an earlier release. Defining one while any node still runs an earlier
  release leaves that node unable to load the table. Nothing is stored
  differently, so removing the names makes the table readable again.
- Naming the components of a table that already holds records reads every record
  inside the defining statement and refuses the statement, defining nothing,
  when one does not fit. An int component holds only ints: a table that holds
  `['a', 5.0]` where the structure needs an int is converted by copying it into
  a new table.
- A retention `DELETE` over such a table takes its cutoff as a value
  (`LET $cut = time::now() - 30d;`), not as a call. A clock read inside a
  mutation's `WHERE` is evaluated per row and bounds no range.
- An aggregated view's plan can read a key range over its group columns, and an
  `UPDATE` or `DELETE` of a view whose `WHERE` on a group alias selects no
  stored row answers `[]` where it raised the refusal that a view is read-only.
- `surrealdb-catalog`, `surrealdb-expr` and `surrealdb-parser-next` gain public
  items that a struct literal or an exhaustive `match` must account for.
- A B-tree index over a field with no declared type that was built by an
  earlier release plans every query as if the field could hold an array:
  `ORDER BY` over it sorts instead of reading the index in order, and a scan
  over it lists each record once rather than stopping at a `LIMIT`, and a
  range open at its upper end (`WHERE time > $since`) reads the table rather
  than the index. Rows are unchanged. `REBUILD INDEX`, once every node runs this
  release, restores the index plans for a field that never held an array.
- A pre-filtered HNSW KNN search no longer widens its `ef`: it reads the new
  `SURREAL_KNN_PREFILTER_HNSW_EF_BOOST`, which defaults to `1`, where it read
  `SURREAL_KNN_PREFILTER_EF_BOOST` (`4`), which now applies to DiskANN alone.
  Such a search also scores the matching records exactly wherever that costs
  less than walking the graph, not only up to
  `SURREAL_KNN_PREFILTER_EXACT_THRESHOLD` (2000) matches. Latency drops for
  most filters and recall holds or rises; see below to keep the previous
  behaviour.
- On a store several nodes share (TiKV, and the enterprise engines), a write to
  a DiskANN index no longer fails with a retryable `TransactionConflict` when
  background compaction of the index runs at the same time. The upgrade is not
  rolling-safe for those indexes: while a node of an earlier release is still
  running, writes made by upgraded nodes can be missing from KNN results.

### DiskANN writes on a shared store, and upgrading every node together

A write to a DiskANN index marks its shard's pending-state guard (`!dy`) as
holding entries, and compaction returns a drained shard's guard to empty so KNN
lookups can skip it. A write could lose its commit to that compaction with a
retryable `TransactionConflict`, which `surreal import` and single-statement
writes do not retry: an import of a table whose `DEFINE INDEX … DISKANN`
precedes its data, which is the order `surreal export` writes, could finish
with rows missing.

Writes no longer touch the guard. On a store that one node opens alone
(RocksDB, SurrealKV, memory) nothing else changes. On a store several nodes
share, each node now holds a writer lease (`!dz`) on the guard shards it writes
into, and no node returns a leased shard's guard to empty. A node releases its
leases once it has written nothing to the index for ten seconds, so a drained
shard's guard returns to empty, and KNN lookups skip it again, shortly after
writes stop. A node that keeps writing renews its leases, and a lease left
behind by a node that crashed or restarted is retired by the first compaction
that finds it 30 seconds past its last renewal. Retirement compares lease expiry times across nodes, so nodes'
clocks must agree to within 15 seconds.

A node of an earlier release does not know about the leases. If it runs the
index's compaction while upgraded nodes write to the index, it can return a
guard to empty under an upgraded node's write, and that write is missing from
KNN results until another write to the same shard. Downgrading a cluster that
upgraded nodes have written to has the same effect.

**What to do**: on a store several nodes share, upgrade every node before
writing to a DiskANN index again: stop writes to DiskANN indexes, or stop the
whole cluster, while the nodes are replaced. On RocksDB, SurrealKV or an
in-memory store there is nothing to do.

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

### Vector indexes gain a doc-ID → elements mapping

A filtered KNN whose filter is covered by other indexes and matches few rows
(the exact prefilter tier) used to read every matching record to score its
vector. An HNSW or DiskANN index stamped with this release's index format keeps
a reverse mapping from each document to the graph elements holding its vectors
(`!hk` / `!dk`), maintained in the same transaction as the index's vector →
documents mapping, and that tier now scores from the index.

An index defined on an earlier release is still read as it is: it has no
mapping, and the exact tier keeps reading its records. Nothing fails and no
result changes.

**What to do**: once every node runs this release, `REBUILD INDEX` the HNSW and
DiskANN indexes that serve selective filtered KNN queries. Do not define or
rebuild a vector index while nodes of an earlier release are still running: an
older node that compacts the index's pending updates does not maintain the
mapping, and filtered queries on the new nodes would then miss or mis-rank the
records it wrote.

### JACCARD and PEARSON rank by distance

KNN orders candidates by ascending distance, but `JACCARD` and `PEARSON` were
passed through as similarities, where higher means closer. A filtered or
unfiltered KNN under either metric, brute force or through an HNSW index,
therefore returned the *least* similar vectors first: the vector identical to
the query came last. (An HNSW index over `F64` vectors happened to use the
Jaccard distance and was the one exception.) Both are now distances in every
KNN path: `1 − J` for Jaccard, lying in `[0, 1]`, and the correlation distance
`1 − r` for Pearson, lying in `[0, 2]`.

Jaccard itself changes meaning. It was computed over the sets of *values* in the
two arrays, ignoring their positions, so `[1, 2, 3]` and `[3, 2, 1]` scored as
identical. It is now the weighted (Ruzicka) Jaccard used for vector search,
`Σ min(aᵢ, bᵢ) / Σ max(aᵢ, bᵢ)` over aligned components, which on 0/1 vectors
is the binary Jaccard of other vector databases. It is defined for non-negative
components: a negative component counts as `0`. `vector::similarity::jaccard`
returns this similarity, `JACCARD` KNN ranks by `1 −` it, and two vectors of
different dimensions are refused.

**What to do**: `REBUILD INDEX` every HNSW index defined with `DIST JACCARD` or
`DIST PEARSON`; its graph was built by the inverted metric and does not serve
nearest-first searches until it is. Review stored or compared results of
`vector::similarity::jaccard` and of `vector::distance::knn()` under either
metric, whose values change.

### Pre-filtered KNN chooses its strategy by cost

A KNN search beside conditions that an index answers restricts itself to the
records matching them (an allow-list). It then either scores those records
exactly, or walks the HNSW graph and admits only matching candidates. The exact
tier was taken up to 2000 matches and the walk above that, with `ef` widened
fourfold. The walk already continues past the candidates the filter rejects, so
the widening multiplied its cost for little recall, and none for a selective
filter; and for an index that keeps a doc-ID → elements mapping, scoring the
matches exactly stays cheaper than the walk well past 2000 matches.

Now an HNSW traversal is widened by `SURREAL_KNN_PREFILTER_HNSW_EF_BOOST`,
which defaults to `1`. `SURREAL_KNN_PREFILTER_EF_BOOST` keeps its default of `4`
and applies to DiskANN alone: with an allow-list alone, DiskANN searches its top
`l` candidates and admits the members among them, so its width still decides
how many matches a selective filter finds. An HNSW search that
can score the matches from the vectors its index stores takes the exact tier
wherever `matches² × SURREAL_KNN_PREFILTER_EXACT_COST` (default `5`) is at most
`ef × m0 × nodes in the graph`. `SURREAL_KNN_PREFILTER_EXACT_THRESHOLD` stays a
floor below which the exact tier is always taken. DiskANN, an index defined
before the doc-ID → elements mapping, and a search that judges each record (a
condition no index answers, or a table permission for a checked caller) keep
the cardinality rule alone. `EXPLAIN ANALYZE` reports the tier taken as
`prefilter_tier`.

**What to do**: nothing, unless a workload relies on the previous search
widths. A deployment that set `SURREAL_KNN_PREFILTER_EF_BOOST` to tune HNSW
searches sets `SURREAL_KNN_PREFILTER_HNSW_EF_BOOST` instead.
`SURREAL_KNN_PREFILTER_HNSW_EF_BOOST=4` restores HNSW's widening, and a
large `SURREAL_KNN_PREFILTER_EXACT_COST` keeps every search above the threshold
on the graph.

### A deeply nested value stored by an older release can error on read

v3.3.0 began rejecting a value nested past the depth cap (256 levels) when
it is written. This release extends the same cap to a `GeometryCollection`, whose
nesting a record's write guard did not previously count, and applies it on the
read path (the public-value conversion every result passes through) as well.

An earlier release could therefore store a value whose overall nesting exceeds
the cap once its `GeometryCollection` is counted, for example a record field grown
by a read-modify-write loop that wraps a collection. Reading such a value back
under this release returns

```
The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records
```

The check runs per request, so a single such row makes every statement that reads
it, including a `SELECT` that scans the table it lives in, return that error until
the row is repaired.

Repair is in place and does not require deleting the row. The write guard checks
the value being written rather than the one already stored, so overwriting the
offending field with a shallower value clears it:

```
UPDATE deep:1 SET g = NONE;
```

Nothing a current release stores can newly reach this state: the write guard now
counts the same `GeometryCollection` nesting the read path does, so a value that
would be unreadable is rejected at the write instead.

### Containment over an element index uses the index only through `.*`

An index on `field.*` is keyed by the elements of each stored array. A
condition such as `field CONTAINS 'x'` is also satisfied by stored values that
hold no element entry for `'x'`: a string or uuid containing it as a substring,
a range spanning it, a geometry containing it, an object with it as a key. An
earlier release answered the condition from the index and silently omitted
those records. This release answers the plain spelling by reading the table,
whether the field has a declared type or not, because a declared type does not
rewrite records stored before it.

The flattened spelling still seeks the index: `field.* CONTAINS v` and
`v INSIDE field[*]` (and the multi-value forms) evaluate a stored scalar to
`NONE`, so it cannot match. A stored object, a record link, or a set in a field
not declared as one is not found through the index under that spelling.

**What to do**: where every stored value of the field is an array, write the
condition with the flatten — `field.* CONTAINS v`, `field[*] CONTAINS v`,
`field.* CONTAINSANY [..]` — to keep the index seek.

### A named record id cannot be read by an earlier release

`DEFINE FIELD id ON reading TYPE [city: string, time: datetime]` stores the
type of the field as the text `[city: string, time: datetime]`, and a node loads
a table's field definitions by parsing that text. An earlier release does not
accept a name inside an array type. On 3.3.0-beta.3 the statement answers

```
Parse error: Unexpected token `an identifier`, expected a kind name
```

An earlier node that loads the same text from the catalog has the same parser,
so a statement that needs the table's fields fails there. A node may hold a
table's definitions in a cache, so the failure can appear on a different node
and later than the `DEFINE` that caused it.

Nothing else about the table differs from a table whose `id` is an unnamed array
type. The record keys, the record bodies, the index entries, the changefeed
entries and the rows of an aggregated view are laid out as before, and a
component is never stored in a record body: it is read from the key. That is why
removing the names restores a table an earlier release can read.

A dump of a database holding a named id carries the names in its
`DEFINE FIELD id` statement. An earlier release fails that statement on import,
and a `DEFINE INDEX` over a component that follows it names a field the earlier
release does not derive.

**What to do**: define names only when every process that opens the datastore
runs this release: every server node, every embedded application, and every
`surreal` command pointed at a datastore path rather than at a server. Leave a
table's `id` as an unnamed array type until then, and restore a dump that
carries names only onto this release.

### Rolling upgrades and going back

The feature changes nothing that is stored until a table's `id` is given names,
so the nodes of a rolling upgrade have nothing to agree on before then. Naming
is the one step an earlier node cannot follow:

1. Upgrade every node to this release.
2. Define the names after the last earlier node has gone, and not before.

Downgrades are not covered by the upgrade tests. To make a table with names
readable by an earlier release, on this release and before the binary is
replaced:

1. Remove every index over a component, and remove or rewrite every view, table
   permission, event and field clause that reads a name, so that it reads the key
   as `id[0]`, `id[1]` and so on. A clause that hands the record to a function
   (`fn::city_of($this)`), calls a function evaluated against the record, or
   reads a field by a name it computes (`type::field($name)`) counts as reading
   every name. Remove every view that selects `*` from the table and holds rows,
   and define it again after the next step. The change in the next step is
   refused while one of these remains, and the error names it. Readers in other
   tables are not found: a clause that reads a component through a record link
   or a graph hop into the table (`prev.city`), or a statement over another
   table that reads this one, has to be found by hand.
2. Remove the names: `ALTER FIELD id ON reading TYPE [string, datetime]`.
3. If the application reads the components as fields, write them into the record
   bodies, which is possible only now that the names are gone:
   `UPDATE reading SET city = id[0], time = id[1];`, which rewrites every record.
   Records written while the names were defined hold neither field, because a
   component is read from the key and never stored. Define the indexes again over
   the stored fields afterwards.

An earlier release then reads the table as an array id with those fields, and
reads no component for a record whose body lacks the field.

### What naming the components checks

`DEFINE FIELD id ON reading TYPE [city: string, time: datetime]` and
`ALTER FIELD id` to a named type read every record of the table before they
define anything. The scan runs in the transaction of the statement and honours
its timeout and cancellation. The statement is refused, and the table is left
exactly as it was, when:

- a record's id is not an array, or an element is not of its component's type;
- a record stores a field under a component's name with a value that differs
  from its key. A stored copy equal to the key is accepted, and is cut at the
  record's next write;
- an `int` position holds a float or a decimal (see the next section);
- a record's id holds a datetime outside the years 0000 to 9999. Such an id is
  stored as text that does not read back as a datetime, so no statement can read
  that record's id, on this release or an earlier one. The refusal says so and,
  because the id cannot be read, names no record:
  ``the existing record in `reading` whose record id cannot be read does not
  match it (a record id holding a datetime outside the years 0000 to 9999 cannot
  be read back)``;
- a name is already a field of the table, or an index or a view over the table
  already reads a stored field of that name, since the name would then mean the
  component instead. An index `COUNT` condition or a view that hands the record
  to a function (`SELECT fn::city_of($this) AS c FROM reading`) may read any
  name, and counts as reading it;
- a view over the table selects every field (`SELECT *`, alone or beside other
  fields) and holds rows. Its rows hold the fields of the records they were
  written from, and naming the components would not rewrite them: rows written
  afterwards would carry `city` and `time` and the old rows would not, so a
  filter on a component would miss them. Remove the view, name the components
  and define the view again, in one transaction, which fills it again from the
  table:

  ```surql
  BEGIN;
  REMOVE TABLE v;
  DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
  DEFINE TABLE v AS SELECT * FROM reading;
  COMMIT;
  ```

  Removing the view removes what is defined on it, which has to be defined
  again. A view that holds no row, an aggregate view and a view that selects
  named fields are not refused for this.

Each refusal names the record or the definition and the remedy:

```
Cannot give `reading` the record id [city: string, time: datetime]: the existing
record reading:['London', 'not-a-date'] does not match it (`time` is not a
datetime). Copy the records into a table with the new record id instead

Cannot give `reading` the record id [city: string, time: datetime]: the record
reading:['London', d'2025-01-01T10:00:00Z'] stores a `city` field that differs
from its record id. Remove it first, for example with UPDATE reading UNSET city

Cannot name `city` in the record id of `reading`: the index `by_city` already
reads a stored `city` field. Remove it, then define it again after changing the
record id

Cannot name `city` in the record id of `reading`: the view `v` reads fields of
the record by names that cannot be determined, so it may already read a stored
`city` field. Remove it, then define it again after changing the record id

Cannot change the record id of `reading`: the view `v` selects every field of
`reading` and holds rows written under the old record id. Remove the view, then
define it again after changing the record id
```

Once a name is part of the id, defining a field of that name is refused in turn.

Only `string`, `int`, `datetime`, `uuid`, `duration`, `bool` and `record`
components are admitted; any other type for a name is refused at definition,
because only those encodings keep their order in storage. A component name must
be a plain identifier: ASCII letters, digits and underscores, not starting with
a digit, and neither `NaN` nor `Infinity`. A keyword such as `time` or `value`
is a plain identifier. A name that has to be escaped, such as `` `the city` ``,
`` `a-b` `` or `` `café` ``, is refused:

```
Cannot name the record id component `the city` of `reading`: a component name
must be a plain identifier of ASCII letters, digits and underscores that does
not start with a digit and is not NaN or Infinity
```

Changing the names later is refused while something reads a name the change
drops or renames, and that includes the `ASSERT`, `DEFAULT` and permissions of
the `id` definition itself, which `ALTER FIELD` keeps, and a field's
`REFERENCE ON DELETE THEN` repair. A clause reads the names it spells, through
`$this`, `$parent`, the document `@` or a destructure (`$this.{city}`) as well
as bare, and in an event through `$after`, `$before`, `$value` and `$input`. A bare name
in a statement over another table (`UPDATE other SET n += 1`) counts as a read
of this table's field of that name, so a component named like a field of the
other table blocks the change, and the error names the event or clause that
holds the statement. A clause that reads the record by a name it computes,
hands the record to a function, a method or a cast, reads a part of a value
that holds the record (`{ r: $this }.r.city`), or calls a user-defined function
where the record is the document it is evaluated against, may read any name,
and blocks the change of every name. The rows a view holds are judged as the
statement that changes the id finds them, including rows written or deleted
earlier in the same block, transaction or function. A view that selects `*` and holds rows blocks it too, with the
remedy above:

```
Cannot change the record id of `reading`: the ASSERT of the field `id` reads its
component `n`

Cannot change the record id of `reading`: the field `label` reads fields of the
record by names that cannot be determined, so it may read its component `city`
```

Comparing the record (`$before != $after`), copying it (`SET copy = $after`),
`$value` in a field's own clause and a builtin handed a field's value do not
block a change. What the check does not see: a read of a component from another
record, through a record link or a graph hop into the table or a statement over
another table that reads this one, whether by a path (`prev.city`) or by a name
passed as a string to a builtin (`object::get(prev.*, 'city')`); and what a
function body reads of a record it is handed as anything but the record itself
(`fn::f(prev)`). A `VALUE` or
`ASSERT` clause that reads the whole record sees at write time only the
components that a field clause of the table reads by name.

A component is a field of the table that only a change of the `id`'s type
removes, so `REMOVE FIELD IF EXISTS city ON reading` is refused with the error
that `city` is part of the record id rather than doing nothing. A migration
script that removes a field which may since have become a component has to
check for that first. The id of a relation
table and of a view cannot be named, and a table with names cannot be turned
into a relation table.

A table too large to be read inside one statement's transaction and timeout
fails the statement with the error of that timeout or limit, and defines
nothing. Copy it with the recipe below: a new table is empty when its id is
defined, so its definition reads nothing.

**Operational notes**: the check reads the records the defining transaction
sees. A write whose statement was planned against the catalog before the
definition committed does not see the names, and can still commit a record
whose id does not fit. Such a record is never returned wrongly, since every read
over a key range keeps its condition and the id is checked as it is decoded, but
a read over a key range can miss it. Define the names while nothing writes ids
that would not fit. Running the same definition again does not check again: a
`DEFINE FIELD OVERWRITE` or `ALTER FIELD` whose names and types equal the stored
ones skips the scan. A missed record shows as a difference between a query's
answer with `WITH NOINDEX`, which never reads a key range, and its answer
without it. Removing the names and defining them again inside one transaction
checks every record again, once nothing reads the names. The same write can
also add a row under the old record id to a view that selects `*`, whether the
check found the view empty or the view was defined again after the change, and
a filter on a component misses that row; removing the view and defining it
again refills it from the table. Define the names while nothing writes to the
table.

### An int component holds only ints, and converting a table that does not

A key stores an int and a float of the same value under different bytes. An
earlier release does not normalise the numbers in a record id: it keeps
`counter:['a', 5.0]` and `counter:['a', 5]` as two records, whether its id was
never typed or is declared `TYPE [string, int]` (observed on 3.3.0-beta.3 and
3.3.0), so a typed table can hold such keys as well as an untyped one. A range over an int
component reaches the bytes of an int and misses those of a float, so this
release keeps the int position to ints:

- a write converts a float or a decimal whose value is a whole number in the range
  of an `int` to that int, and refuses any other number with an error that begins
  ``Couldn't coerce value for the record id component `n` of `counter`:``;
- defining the names refuses a table that already holds a float or a decimal at
  an int position, and the error names the first such record.

Both apply only to a table whose `id` names its components. A plain upgrade
changes no record and needs no action; only the statement that names an `int`
component can be refused.

**What to do**: copy the records into a new table whose `id` is defined first.
Explicit ids are converted as they are written, so the copy keeps each record's
key and turns `5.0` into `5`. Two records that convert to the same key are one
record in the new table, and the copy has to say what becomes of them.

A table `counter` holds four records, written by an earlier release:

```surql
counter:['a', 4]    hits: 1
counter:['a', 5]    hits: 2
counter:['a', 5.0]  hits: 3
counter:['b', 7.0]  hits: 4
```

```surql
DEFINE FIELD id ON counter TYPE [site: string, n: int];
-- Cannot give `counter` the record id [site: string, n: int]: the existing record
-- counter:['a', 5f] does not match it (`n` is a float; an int component holds only
-- ints). Copy the records into a table with the new record id instead
```

The new table carries the structure from the start. It holds no record, so
defining it checks nothing:

```surql
DEFINE TABLE counter_v2 SCHEMALESS;
DEFINE FIELD id ON counter_v2 TYPE [site: string, n: int];
```

A counter sums the records that collide. `INSERT INTO ... (SELECT ...)` keeps the
key of each record it reads and replaces its table, and `$input` is the row being
inserted:

```surql
INSERT INTO counter_v2 (SELECT * FROM counter)
    ON DUPLICATE KEY UPDATE hits += $input.hits;
```

Check the result before anything depends on it. The records that merged are the
difference in count, and the sum is unchanged:

```surql
SELECT count() AS records, math::sum(hits) AS total FROM counter GROUP ALL;
-- [{ records: 4, total: 10 }]
SELECT count() AS records, math::sum(hits) AS total FROM counter_v2 GROUP ALL;
-- [{ records: 3, total: 10 }]
SELECT site, n, hits FROM counter_v2 ORDER BY id;
-- [{ hits: 1, n: 4, site: 'a' }, { hits: 5, n: 5, site: 'a' }, { hits: 4, n: 7, site: 'b' }]
```

Without `ON DUPLICATE KEY UPDATE`, the first collision fails the statement with
``Database record `counter_v2:['a', 5]` already exists``. Outside a `BEGIN`
block a statement is one transaction, so the failed copy stores nothing and can
be run again.

Where one of two colliding records should win rather than merge, copy the
records that win first and skip the rest, which `INSERT IGNORE` does for a record
that already exists. Here the record written as an int wins, and the whole copy
commits or none of it does:

```surql
BEGIN;
INSERT INTO counter_v2 (SELECT * FROM counter WHERE type::is_int(record::id(id)[1]));
INSERT IGNORE INTO counter_v2 (SELECT * FROM counter WHERE !type::is_int(record::id(id)[1]));
COMMIT;
```

`record::id(id)[1]` is the second element of the key, on any table. A number
that is not whole, such as `['b', 7.5]`, is neither merged nor skipped: its copy
fails with the coercion error above, because no int is that key. Decide which key
the record should have and insert it under that id.

Then switch the application to `counter_v2` and remove `counter`. What the copy
does not carry is everything defined on the old table, which a new `DEFINE TABLE`
does not inherit: restate its permissions, fields, indexes and events on the new
table, using `INFO FOR TABLE counter` as the list. Records elsewhere that link to
`counter:['a', 5.0]` keep pointing at the old table and have to be rewritten.
A copy into the old name goes through a staging table instead:
`INSERT INTO staging (SELECT * FROM counter)` keeps every key as it is, then
`REMOVE TABLE counter`, define the table with its new `id`, and run the copy
above from `staging`. It holds the data twice for the time between.

A table too large for one transaction is copied in slices by a condition on the
first component, for example `WHERE record::id(id)[0] = 'a'`, one statement per
slice. Records that collide share every component, so a slice keeps them
together. A failed slice leaves the earlier slices in the new table, so empty it
(`DELETE counter_v2;`) before running the copy again.

### Retention over a keyed table

A retention delete takes its cutoff as a value:

```surql
LET $cut = time::now() - 30d;
DELETE reading WHERE time < $cut;
```

A mutation evaluates a clock read in its `WHERE` on every row, and every read
answers the one instant of the statement. `DELETE reading WHERE time <
time::now() - 30d` therefore removes the same records in effect, but a
comparison with the clock bounds neither a key range nor an index over a
component of the id: it reads every record of the table and calls the clock for
each. `LET` evaluates the clock once, before the statement, and `$cut` is a
value the statement holds, which can bound a key range. Over a field that is no
component of the id, a `SELECT` offers a comparison with the clock to an index
as a bound that reads the statement's instant when the scan opens, so `seen >
time::now() - 1d` needs no `LET` there.

A bound as a value reads a range only where the key order allows it. In an id
`[city: string, time: datetime]` the keys are ordered by city and then by time,
so `time < $cut` is one contiguous range for a city and not for the table. There
are three ways to reach the old records without reading the rest:

- pin the leading components: `DELETE reading WHERE city = $city AND time < $cut;`,
  once per city;
- define an index on the component, `DEFINE INDEX by_time ON reading FIELDS time;`,
  which `DELETE reading WHERE time < $cut` reads as a `SELECT` does;
- order the key by time, `[time: datetime, city: string]`, when time is what
  most reads and deletes bound. A range over the first component needs no
  prefix, and a read by city alone then has no leading component to bound it.

The planner does not skip over a leading component to reach a later one, so a
bound on `time` alone over `[city, time]` without an index reads the whole table.
That is correct and costs the table's size, and the `LET` form still saves the
clock call on every record. A bound that a client supplies as a query variable is
the same as a `LET`.

### Aggregated views read key ranges

The rows of `DEFINE TABLE hourly AS SELECT ... GROUP BY city, hour` are keyed by
an array of the group values. A `WHERE` that compares the group columns to
literals now reads the matching range of that table's keys and no more, and keeps
the condition above the read. A view row's group columns are stored fields, so
nothing is derived and no definition changes. The rows a statement returns are
unchanged; `EXPLAIN` shows a key range where it showed a scan.

One answer changes. An `UPDATE` or `DELETE` of a view whose `WHERE` on a group
alias selects no stored row returns `[]`, where the full scan raised the refusal
that a view is read-only. A statement that selects a row still raises it.

### Changes to the published crates

`surrealdb-catalog`, `surrealdb-expr` and `surrealdb-parser-next` are published.
Code that builds or matches on their types has to name the additions:

- `surrealdb_parser_next::Entry` has a new variant, `IdKind`. An exhaustive
  `match` on `Entry` needs an arm.
- `FieldDefinition` (`surrealdb-catalog`) gains the public fields `key` and
  `key_part`, and `DefineFieldStatement` and `AlterFieldStatement`
  (`surrealdb-expr`) gain `key_names`. A struct literal names them, or takes
  `..Default::default()` where the type provides it.
- A table's field list can hold a virtual definition for a component, which has
  `key_part` set and no stored form. Code that lists the definitions and writes
  them back must skip it.
- `INFO FOR TABLE ... STRUCTURE` reports an `id` with named components under an
  additive `components` key, a list of `name`, `position` and `kind`. Its `kind`
  keeps the unnamed form (`[string, datetime]`), so a client that parses `kind`
  is unaffected.

### Index plans follow what an index has stored, not a field's declared type

The index writer stores a record whose indexed field holds an array once per
element of the array, so such an index's entries do not stand one for each
record, and their order is not the order of the field's values. `DEFINE FIELD`
does not rewrite records stored before it, and an import skips coercion, so a
field declared `TYPE int` can still hold an array written earlier: an earlier
release trusted the declared type, and such a record could be counted, returned
or ranked once per element, or match a condition it does not satisfy.

A B-tree index built on this release records, per column, whether it has
stored an array, and the planner trusts that record alone, typed field or not.
For a column that never has, the index delivers the column's order, a `LIMIT`
stops the scan, a range over the column is answered by its keys, `count()`
counts index keys, and a KNN query narrows its search exactly to the
condition's matches. The first array written to the column changes every plan
over it from then on. Deleting the record does not change it back; rebuilding
the index does.

A column that may hold an array is also not ranged over by its index past the
place arrays sort, after every scalar: a record holding `[1]` satisfies
`time > 5` by its whole value, but its one entry sits at `1`, outside the range.
Such a range — one open at its upper end, or bounded above by an array or a
later value — reads the table instead.

An index built by an earlier release recorded nothing, so this release plans
all its columns as columns that may hold an array. Every query still returns
the same rows, but:

- `ORDER BY` over an indexed field sorts the matching rows: `SELECT * FROM
  event ORDER BY time DESC LIMIT 10` no longer reads just ten index entries,
  and `WHERE time > $since` reads the table.
- `count()` over an indexed condition reads the matching records rather than
  counting index keys (about five to seven times slower at 1–20% of a table).
  A `COUNT` index whose condition is the query's still answers from its
  counter.
- Range and compound scans remove duplicate records, which adds a small
  constant to each scan.
- A KNN query (`<|k,ef|>`) combined with a range condition checks that
  condition during the graph search rather than narrowing the search
  beforehand, so a selective range can return fewer than `k` rows and lower
  recall.

A statement that may write — `UPDATE`, `DELETE`, a block holding a write, or
any statement calling a user-defined function or a script — plans every column
as one that may hold an array, rebuilt or not: it is planned before its writes
run, and a write can store the first array.

**What to do**: nothing is required for correct results. To restore the index
plans, once every node of the cluster runs this release, run `REBUILD INDEX` on
each B-tree index whose queries order by, count over, or take ranges over its
fields. A node still on an earlier release writes arrays into an index without
recording them, so an index rebuilt while such a node is still writing can
plan a column that holds an array as one that never has, and order or bound it
by element. For a KNN query with a selective range condition on an index not
yet rebuilt, a larger `ef` recovers recall at some cost.

## v3.3.0 — datastore version stamp, startup data migrations, the sequence keyspace move, module re-keying, rollbacks reported as `cancelled`, and ANN pending entries at a new revision

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
