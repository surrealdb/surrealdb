# VM storage instructions: writes and schema statements as bytecode

**Status:** draft. Settles how writes, schema statements (`DEFINE`, `REMOVE`, `ALTER`, `REBUILD`)
and `INFO` run on the VM: compiled by the planner into bytecode made of generic instructions — KV
operations on the statement's transaction, key and value codecs, expressions, branches — with no
call into a Rust statement body and no second evaluator. `VM_WRITES.md` has the
per-record control flow this builds on.

## 1. The rule

A statement's program is everything the statement does. The host provides the transaction, the
storage cursor and the services listed in §6, and nothing that decides what a statement means.
A catalog read or write is a KV get or put of an encoded key and an encoded value; the VM
encodes both itself.

## 2. The stored-value codecs

A stored value is `revision` 0.30 bytes, or a catalog entry in the stored grammar. A record, an
index entry's payload and the datastore's bookkeeping values are `revision` bytes (a record may
also be in the v1 record envelope). A catalog definition is a stored-grammar entry when the store's
feature level allows it, and `revision` bytes when a 3.x binary wrote it or the level is still
`Legacy`; both are read, so an entry stays readable until an ordinary write of it replaces it. The
VM reads and writes all of them itself; nothing hands a decoded Rust struct across.

### Catalog entries

An entry is `0xFE`, a header byte, a varint read floor, then a keyed map from field number to value
(`catalog/src/stored/entry.rs`). The floor is the newest release an entry needs: the encoder
computes it from the fields the entry sets and from what its values use, and a reader refuses an
entry above its own release with an error naming the release. A catalog entry never starts with a
byte a `revision` value can start with, so the first byte selects the decoder.

Every catalog type is declared once, by `#[derive(Stored)]` on the Rust type: per field its number,
wire type, the release that added it and its class, which is the number's parity (even is critical,
odd is ignorable). The derive generates the encoder, the decoder and the VM's codec for the type, so
the VM's bytes are the Rust type's bytes. The VM reaches them through one `CatalogVTable` per type
(`catalog::stored::CATALOG_TYPES`): `Decode` reads an entry in either grammar into a generic value,
`Encode` writes a value at the store's feature level and over the entry it replaces, which keeps
the fields a newer release added. A catalog value held inside a `revision` container (a table
change's definition) is read and written through the same table, in the form the container holds
it. The numbering is frozen by `catalog/catalog-schema.lock`, which CI checks against the base
branch's copy.

### What the `revision` format is

| piece | encoding |
|---|---|
| unsigned integer | varint: `< 251` one byte; `251` + u16 LE; `252` + u32 LE; `253` + u64 LE (u128: `254` + u128 LE) |
| signed integer | zigzag, then the unsigned varint |
| `bool` | one byte, `0`/`1` |
| string, bytes | varint length + bytes |
| `Option<T>` | tag byte `0` (none) / `1` + `T` |
| `Vec<T>`, maps | varint length + elements (`specialised-vectors` bulk-encodes `Vec<u8>`) |
| struct, plain revision | u16 revision varint, then the fields that revision has, in declaration order |
| enum, plain revision | u16 revision varint, u32 discriminant varint, then the variant's fields |
| struct, `optimised, indexed_struct` | revision varint, `u32` LE payload length, a prologue of `u32` LE field offsets, then the fields — any field reachable in O(1) (`Record` rev 2) |
| enum, `optimised` | revision varint, then a one-byte tag holding the variant id and its size class (`inline`, `fixed(n)`, `varlen` with a `u32` LE length), then the body — any value skippable in O(1) (`Value` rev 2) |
| `Object`/`Array`/`Set` rev 2 | indexed prologues: binary search over keys, O(1) element access |

Decoding an older revision reads the fields that revision had and fills the rest: a field added
later takes its default; a field removed later is read and dropped, or handed to its
`convert_fn`, which rebuilds the current field from it.

### How the VM holds a `revision` type

A **codec descriptor** per revisioned type that no catalog schema covers: for each revision it can read, its framing
(plain, optimised, indexed), its fields or variants with their types and discriminants, size
classes, defaults for fields it lacks, and conversions for fields it dropped. Descriptors are
data, so a program that ships to a worker carries or references them like any other constant.

Instructions over them:

- `Decode { ty, src, dst }` — decode a whole value of `ty` into a register.
- `DecodeField { ty, path, src, dst }` — walk an `optimised`/`indexed` value to one field without
  decoding the rest; the path is field offsets and variant tags, resolved at lowering.
- `Encode { ty, src, dst }` — encode at the type's current revision.
- `Skip { ty }` — for walking sequences.

A descriptor's `convert_fn` is VM code. There are nine: `StoredSubscriptionFields` and
`StoredSubscriptionDefinition` (select, fetch), `StoredFieldDefinition` (kind, computed
dependencies), `StoredFunctionDefinition` (args, returns), `RecordType::upgrade_edge_v1`, and
`Relation`'s `from`/`to` in `expr/src/expr/table_type.rs`. Each is small and pure, so each is
written once as a bytecode function the descriptor names. A `default_fn` (auth limits, event
kind, a module's `unsigned` flag, aggregation shifts) is a constant or a small function the same
way.

### Revisions that store a SurrealQL AST

Before 3.3, field kinds and function argument and return kinds were stored as the structured
`Kind` enum. The Rust codec converts those revisions on read, so the catalog's codec does too. A
migration (`kvs/migration/kinds_as_text.rs`, migration 4) re-saves every such definition at its
text revision once, on upgrade, so no read converts one again. It is safe to run during a rolling
upgrade because every 3.3.x node reads the revisions it writes, and the supported upgrade path
starts at the latest 3.3.x. A live subscription's revision-1 fields and fetch clauses are
converted on read the same way; a subscription is removed with the node that registered it, so
none is migrated.

### Where descriptors come from

A catalog type's codec is generated by its `#[derive(Stored)]`, which also generates the type's
`TypeSchema`; there is no descriptor to keep in step. The types a catalog value embeds that are
defined outside `catalog/src` (`TableType`, `Relation`, `ChangeFeed`, `Filter`, `Tokenizer`,
`ScramCredential`, the iam types) derive the same way, and the stored SurrealQL text wrappers
(`SurqlText<T>`) are a string whose floor comes from the release the parsed text needs.

Every other revisioned type has a hand-written descriptor (`exec/vm/codec/types.rs`): the ids
(`NamespaceId`, `DatabaseId`, `TableId`, `IndexId`, a bare `u32` with no revision header),
`Record`'s own encode and decode (a depth check; the key's id spliced into the data), and the
datastore's values (`ReclaimState`, `CatalogVersion`, `SequenceState`, `BatchValue`,
`IndexBuildState`). A change to one of their revisioned types changes the descriptor in the same
commit.

### Verified, not trusted

- Every frozen fixture in `core/src/kvs/compat/` (`v3_0_0_beta_1` … `v3_4_0`) decodes through
  the VM to the value the Rust codec decodes it to.
- Encoding is byte-identical to the Rust codec: a property test encodes generated values of
  every hand-written descriptor both ways and compares bytes, and decodes each through the other.
  A catalog type needs no such test, because its codec and its Rust type are one declaration.
- `revision.lock` changing forces a descriptor change in the same commit, for a hand-written
  descriptor. `catalog/catalog-schema.lock` changing is checked by `cargo make ci-catalog-schema`:
  a field number is never reused or retyped.

### What the VM does not need to do with stored text

A stored expression — a field's `VALUE`, a permission predicate — is SurrealQL text. The
planner compiles it when it binds the statement, reading the catalog at plan time, and the
bytecode it emits is what runs. At run time the VM only carries such text as bytes: `INFO`
renders it, `DEFINE` writes it. The VM never parses SurrealQL.

## 3. Keys

Every key is declared once, in the `keyspace!` schema (`datastore/src/key/schema.rs`, map in
`datastore/src/key/keyspace.map`): a route of literal tags and typed segments — `fixed(n)` integers
big-endian (signed ones with the sign bit flipped, so bytes sort as numbers), `var` segments in
`storekey` encoding, zero-terminated with interior zeros escaped — and the value type stored
under it. The macro
already generates each key's encoder, decoder and scan bounds, and checks that no two routes can
claim the same bytes.

The VM gets a **key descriptor** per route, emitted by the same macro: its segments, their
encodings, its `@` truncation points (scan bounds), and the codec descriptor of the value it holds.

- `Key { route, segments, dst }` — encode a key from registers.
- `KeyRange { route, prefix, dst }` — the scan bound ending at a truncation point.
- `DecodeKey { route, src, dst }` — a key back into its segments, for a range scan that needs
  them.

Because the value type rides on the route, `Get { route, key }` decodes what it reads with the
right descriptor and `Put { route, key, value }` encodes with it: a program cannot put a
`TableDefinition` under an index key.

## 4. Writes

The per-record control flow is VM code already (`VM_WRITES.md`). What remains is each stage's
body, and nearly all of it is KV operations and expressions:

| stages | lower to |
|---|---|
| guards (`CheckRecordExists`, `CheckTableNotView`, `CheckTableNotLightweight`, `CheckTableType`), `CheckPermissionsQuickCreate`, `CheckRecordUserAccess`, `CheckDataFields`, `DefaultRecordData`, `ClearRecordData`, `CleanupTableFields` | branches over the row and the definitions the planner bound; no KV |
| `CheckWhereCondition`, `ComputeInputData`, `ProcessMergeData`, `ProcessRecordData`, `GenerateRecordId` | expression bytecode and the five mutators (`set`, `del`, `increment`, `decrement`, `extend`) as instructions over the row |
| `ProcessTableFields`, `MaterialiseObservedFields`, the table and select permission gates, `OutputWrite` | expression bytecode, compiled once per statement by the planner — the field clauses are compiled per record today |
| `StoreRecordData`, `PurgeRecordData`'s record delete | `Put`/`Set`/`Delete` on `RecordKey`, with the record-cache rule below |
| `StoreIndexData` for unique and plain indexes, `ProcessTableReferences`, `CleanupTableReferences`, edge keys in `StoreEdgesData`/`StoreInlineAdjacencyData`/`purge_pointers` | key encoding, compare-and-put/delete, set-diff of old and new values |
| edge and reference cascades, event `THEN`, view recalculation | nested programs — the planner compiles the nested `DELETE`/`UPDATE`/`SELECT` it builds today, once per statement |

### What is not a KV operation

These are the instructions the storage set needs beyond get, put, delete and range:

1. **Commit-time buffers.** Six things are appended to the transaction and written at commit:
   change feed rows, live-event rows, index count deltas, compaction triggers, graph fold
   triggers, and the async-event flag. One `Buffer { kind, … }` instruction per kind; the
   commit does the rest, as it does now.
2. **The transaction's record and definition caches.** `put_record` inserts, `set_record` and
   `del_record` evict. A `Put`/`Set`/`Delete` on a cached route applies the same rule, so a later
   read in the transaction sees the write (read-your-own-writes).
3. **Sequences.** Doc ids and table ids come from node-local batches leased in separate
   transactions. `NextId { sequence }` is a host service: allocation is not part of the
   statement's transaction and must not roll back with it.
4. **Index builder admission.** A write to a table whose index is being built reserves a ticket
   in a separate compare-and-set transaction. The same shape as sequences: a host service.
5. **Live-query notification.** A channel send to the broker, after the subscription's `WHERE`
   and projection ran as bytecode under the subscriber's auth.
6. **Clocks and randomness.** `Utc::now`, the process-wide HLC for async events, random and
   UUIDv7 ids — each an instruction, so a program's non-determinism is visible in it.
7. **Index maintainers beyond B-trees.**
   - HNSW and DiskANN: the write path is one pending-key read-modify-write plus a compaction
     trigger; the graph is built by background compaction. Lowered as KV.
   - Count: a buffered delta. Lowered as KV plus `Buffer`.
   - Full-text: the analyzer (tokenizer, filters, segmenter, about 2.5k lines) and the term and
     doc buffers. The hard one — see open questions.
   - Materialised aggregate views: `aggregation_stats` arithmetic (about 1.7k lines in
     `catalog/src/aggregation.rs`).
8. **In-memory index handles outside the transaction.** `IndexStores` holds HNSW/DiskANN
   handles and the adjacency resolve cache; the inline edge/reference caches backfill with a
   range scan on a miss.

### Where expressions have no stored logical form

Stages never call a Rust statement body themselves. A stored clause with no logical form is
lowered from its stored text where it is reached (`exec/permission.rs`), and three always are:

- a field's `PERMISSIONS FOR create/update` — the catalog stores no logical form for it;
- a materialised view's projection;
- a `LIVE SELECT` projection.

An event's `THEN` usually is, and every other clause is when it has no logical form. These close
with expression bytecode, not with the storage set.

## 5. Schema statements and `INFO`

The planner lowers each statement into what its body does today. Every body has the same shape:

1. **Permission.** `is_allowed(action, resource, base)` against the session's auth — an
   instruction over the auth the program runs under.
2. **Names.** A bare identifier is the name itself; anything else is an expression, evaluated and
   coerced to a string. `DEFINE PARAM`'s value, durations, keys and comments are expressions too.
3. **Resolve the namespace and database.** Read `/!ns{ns}` and `/*{ns}!db{db}`; several forms
   create them when missing (`get_or_add_*`), which is a get, an id from the sequence service, and
   a put.
4. **Read what exists.** `Get` of the definition's key.
5. **Branch** on `IF NOT EXISTS`, `OVERWRITE`, `IF EXISTS`, or raise `*AlreadyExists` /
   `*NotFound` (an import skips the first).
6. **Validate** — cross-object checks read other definitions with range scans (`all_tb_fields`,
   `all_tb_indexes`, `all_tb_views`): computed-field cycles, lightweight-relation rules, inline
   field limits, analyzers in use.
7. **Build the definition** from registers, encode it, `Put`/`Set`/`Delete` it.
8. **Stamp the table.** Every per-table change ends with a locked read and rewrite of `!tb{tb}`
   (`replace_tb`), which bumps `/*{ns}*{db}!cv{tb}` — the catalog version plans depend on — and
   the table's `cache_*_ts`.
9. **Invalidate** the transaction's catalog cache; three forms also clear the datastore cache.

Most statements are only these steps: `DEFINE`/`REMOVE`/`ALTER` of `NAMESPACE`, `DATABASE`,
`PARAM`, `FUNCTION`, `EVENT`, `FIELD`, `USER`, `ACCESS`, `API`, `CONFIG`, `SEQUENCE`, `MODEL`,
`MODULE`, `ANALYZER`, `BUCKET`, and every `INFO`, which is range scans plus decoding plus
building its object. The ones with more:

| statement | beyond the steps |
|---|---|
| `DEFINE TABLE … AS` (a view) | range-delete the view's keyspace, then backfill it by running its `SELECT` — a nested program — and writing each row through the view-maintenance order |
| `DEFINE INDEX`, `REBUILD INDEX` | the index builder: a backfill in separate transactions on a spawned task, which a blocking statement waits for (§6) |
| `REMOVE TABLE` | range-delete of the table's whole keyspace, in the statement's transaction |
| `REMOVE NAMESPACE`/`DATABASE`/`INDEX` | put a reclaim entry (`/!rc…`) that background reclaim acts on |
| `DEFINE`/`REMOVE FIELD` | range scan of reference keys to purge the ones a dropped reference left |
| `REMOVE USER`, removals of views and tables | kill live subscriptions: range scans of `!lq` and a kill notification sent on commit |
| `DEFINE ANALYZER` | load mapper files and segmenter dictionaries (§6) |
| `DEFINE BUCKET`, `DEFINE MODULE`, `REMOVE MODEL` | connect an object store, load a WASM module, delete an object-store file (§6) |
| `ALTER SYSTEM`, `ALTER … COMPACT` | set the process query timeout; storage compaction (§6) |

Password hashing, SCRAM credentials and random user codes are produced when the statement is
converted, not when it runs, so they arrive as constants.

### A schema statement in the plan and in the program

The physical plan is shaped for queries, and a schema statement is not bent into it: each maps to
one plan node carrying the statement's constants — its clauses in the canonical text the catalog
stores, converted once when the statement is planned, like any literal — and its premises. Its
steps appear when it is lowered, as instructions:

| instruction | does |
|---|---|
| `RefuseNoWrite`, `Allow { action, resource, base }` | refuse a frame that refuses writes; the permission check |
| `SessionNs`, `SessionDb`, `SessionAuthLimit` | the session's names, and the auth limit a definition records |
| `NextId { sequence }`, `NewUuid` | an id from the sequence service; a version-7 uuid for a cache stamp or catalog version |
| `Get`, `Scan` | catalog reads: one key, or every value beneath a truncation point |
| `Lock` | a locked read, so a concurrent writer of the key cannot also commit |
| `Put`, `Replace`, `Delete`, `DeleteRange` | the catalog writes; `Clear` and `ClearRange` under `EXPUNGE` also drop every earlier version |
| `Increment` | a counter, such as the changefeed retention fence |
| `Next`, `Push`, `DropMethods` | step through a scanned list; edit a stored list |
| `Raise`, `JumpIfImport` | the statement's errors; an import passing over an existence error |
| `ClearCache`, `ClearDatastoreCache` | evict the transaction's, or the datastore's, cached definitions |
| `Compact`, `CompactAll` | ask the store to compact a subtree, or everything |

Resolving the namespace and database — creating them where the body does — and the existence
branch are ordinary control flow over `Get` results; bumping a scope's catalog version is a `Put`
of a new uuid at its `CatalogVersionKey`. A definition is a constant built when the statement is
planned, in the stored form, with the holes the program fills from its reads; an `ALTER` reads the
stored definition and replaces the altered clauses with theirs.

**Walks run in the transaction.** A removal that cascades — `REMOVE NAMESPACE` retiring every
index of every table of every database, killing their subscriptions, evicting their sequences —
walks the catalog with `Scan` and `Next` when it runs, not from what the planner read. An earlier
statement of the same transaction may have added what the walk has to reach. The per-item
effects outside the store (evicting an index's loaded state, an `on_commit` build abort or
`KILLED`, a sequence eviction) are host services taking the scanned definition; a walk whose
service the datastore runs without is skipped whole (`JumpIfNoService`).

**Effects follow the permission check.** A verifier over every lowered schema program refuses one
in which an instruction with an effect — `Resolve { create: true }`, a write, an id from the
sequence service — precedes its `Allow`. The current bodies break this in places (§9: `DEFINE
FIELD` may create the namespace and database before its check); the lowering orders them
correctly, and the verifier keeps it so.

Within one statement there is little else to gain: `is_allowed` is an in-memory check, and
repeated catalog reads hit the transaction's cache. The one optimization that pays spans the
statements of a block, a script or an import — each per-table definition rewrites the table
definition under a locked read and clears the whole transaction cache, once per statement — and
it is a pass over the block's program: a scope's `Stamp` and `Invalidate` may be deferred to just
before the next read of that scope, or to commit, never past a read.

### Cached programs and their premises

A program records its premises as the plan's `dependencies`: each catalog scope it read, with the
version stamp read. Before a cached program runs, the host reads those stamps in the statement's
transaction — one batched get — and re-plans on any mismatch. Reading through the transaction is
what keeps a cached program from running against a catalog change made earlier in the same
transaction.

What that needs beyond today's per-table stamp (`!cv{tb}`):

1. **A stamp for every scope.** Database-, namespace- and root-level objects — functions, params,
   analyzers, access methods, users, config, sequences — have none, and some writes of per-table
   keys rely on a following `replace_tb` for theirs. Every catalog write bumps its scope's stamp.
2. **Premises that are not definitions.** An index's build state decides whether a program may
   read it, so it is a dependency, stamped by the builder. Statistics are not premises: a stale
   one costs speed, not correctness.
3. **Premises from the caller.** The principal, the permission predicates bound for it, and the
   session values folded at plan time are part of the cache key, not dependencies.
4. **One verifier.** The host checks the dependencies before the program runs; a program never
   re-checks its own premises.

## 6. Services the host provides

Things a program asks for that are not operations on its transaction's keys. Each is an
instruction whose effect is the service's, so a program's side effects are all visible in it.

| service | used by | why it is not a KV op |
|---|---|---|
| commit-time buffers | change feeds, live events, count deltas, compaction and graph-fold triggers, async-event flag | written by the commit, with the commit's versionstamp |
| `on_commit` / `on_rollback` actions | subscription kills (`KillOnCommit`), aborting or cleaning an index build (`RetireIndexLocally`) | run after the transaction resolves |
| sequences | namespace, database, table, index and doc ids | node-local batches leased in separate transactions; ids are not returned on rollback |
| index builder | `DEFINE`/`REBUILD INDEX`, writes during a build | spawned task, separate transactions, a lease on `!bs` |
| live-query broker | subscription notifications | a channel send |
| clocks and randomness | `time::now`, the HLC for async events, random and UUIDv7 ids | non-deterministic |
| in-memory index handles | HNSW/DiskANN handles (`RetireIndexLocally`), adjacency and inline caches, analyzer mappers and segmenters (`LoadAnalyzer`, `CleanupMappers`), sequence state (`EvictSequence`) | process state outside the transaction |
| object stores, module runtime | buckets (`ConnectBucket`), models, modules (`EvictModule`, `LoadModule`) | network and filesystem |
| process configuration | `ALTER SYSTEM` (`SetQueryTimeout`), storage compaction | process-wide, not transactional |

## 7. Order of work

1. The codec: descriptors, `Decode`/`DecodeField`/`Encode`, verified against every compat
   fixture and by round-trip property tests. Keys from the keyspace schema.
2. `INFO` — reads only, and the fastest check that decoding is exact.
3. `DEFINE`/`REMOVE`/`ALTER` of the plain forms, starting with `TABLE` and `FIELD`.
4. Write stage bodies, in the order of §4's table: guards and data, then the record and
   B-tree/unique index stores, then references and edges, cascades, events, views, lives.
5. `DEFINE INDEX` with the builder, the full-text maintainer, aggregated views.

Each step lands when the language suite passes and a test pins the stored state the statement
leaves.

## 8. Open questions

- **Descriptor generation.** `revision` is the external `surrealdb/revision` crate; emitting
  descriptors from its derive means a release of it. Until then, hand-written descriptors checked
  against the compat fixtures.
- **Full-text analysis** (about 2.5k lines of tokenizer, filters and segmenter) as bytecode, or as
  a service like the analyzer mappers it already loads.
- **Aggregated-view statistics** (`catalog/src/aggregation.rs`, about 1.7k lines) as bytecode.
- **Stored field permissions** have no logical form in the catalog, so they are lowered
  from their stored text when reached; the catalog has to store one before they can be compiled
  with the definition.

## 9. Divergences found in the current bodies

Found while taking the inventory; each is the current behaviour, which the lowering reproduces
until it is fixed at the source.

- `ALTER CONFIG DEFAULT` checks the database base and writes the database config key; `DEFINE`
  and `REMOVE CONFIG DEFAULT` use the root key. It also writes when the config is missing and
  `IF EXISTS` was not given.
- `DEFINE FIELD` evaluates its clauses, and may create the namespace and database, before its
  permission check; `DEFINE EVENT` and `REMOVE ANALYZER` do part of their work before theirs.
- `ALTER ACCESS` checks resource kind `Access`; `DEFINE`/`REMOVE ACCESS` check `Actor`.
  `INFO FOR INDEX` checks `Actor`; every other `INFO` checks `Any`.
- `REMOVE TABLE` deletes its keyspace in the statement's transaction; namespace, database and
  index removal defer to reclaim.
- `REMOVE MODEL` deletes the object-store file before the transaction commits.
- `DEFINE SEQUENCE … OVERWRITE` and `ALTER SEQUENCE` leave the in-memory sequence as it was;
  `ALTER BUCKET` does not reconnect and `REMOVE BUCKET` does not drop the connection.
- `ALTER FIELD` runs none of `DEFINE FIELD`'s computed, cycle, type or reference validators.
