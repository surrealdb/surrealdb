# RFC: Record layouts, and migration

**Status:** built (ADR 0009 for the layouts and generations; §§5–6 for migration). §9 lists what
is open.

Companion: [`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md), on which a
migration's backfill runs as a triggered program; [`RFC_RECORD_FORMAT_ROLLOUT.md`](RFC_RECORD_FORMAT_ROLLOUT.md),
which says how a store reaches the format. The decision record is
[`../adr/0009-the-4-0-record-format.md`](../adr/0009-the-4-0-record-format.md). Requirements this
builds on: [`PHYSICAL_PLAN_REQUIREMENTS.md`](PHYSICAL_PLAN_REQUIREMENTS.md).

---

## 1. Why

A record in every release before 4.0 is one KV value holding a `revision`-encoded `Record`: a
self-describing tree with a tag on every value. Reading one field means walking the encoded object
and decoding the field through the generic deserializer; the `where_the_time_goes` probe in
`core/src/exec/vm/tests.rs` shows that locating and decoding one integer costs more than the
storage cursor that produced the row. Nothing about a record's shape is known before it is read, so
no field has a known offset, no batch of rows has a column to load, and every read pays for
generality the schema already rules out.

The VM reads rows only through `RowReader` (`core/src/exec/vm/row.rs`), so a new encoding is a new
implementation behind that interface, not a change to the VM.

## 2. Principles

1. **One logical record, several physical layouts.** A table's rows mean the same whatever layout
   stores them; the layout is a performance choice, and a reader answers the same value whichever
   layout it meets.
2. **No opt-in.** A `SCHEMAFULL` table packs automatically: its layout is derived from its field
   definitions, and a row a column cannot hold is written as a document instead. Every other table
   writes documents. Nothing a user sets chooses a layout in 4.0.
3. **Correctness never depends on the migration guarantee; speed does.** A reader learns each row's
   layout and generation from its first bytes, so a table that mixes `revision`, document and
   packed rows always reads correctly. The table's floor and its conformance record answer a
   different question, for the planner: whether it may compile a typed scan over one layout with no
   `NONE` or kind handling.
4. **A migration never fails and never writes on its own behalf.** Its transform is proven pure and
   total before it is accepted (§5), which is what lets a record be migrated lazily, on read, any
   number of times, with the same answer the backfill writes.

## 3. Layouts

A record is stored in one of three ways, told apart by its first bytes. `catalog::StoredRecord`
(`surrealdb/catalog/src/record.rs`) holds the byte-level contract.

| first bytes | layout | a KV value holds | read through |
|---|---|---|---|
| anything but `0xFF` | `revision` (every 3.x record) | one `Record` | the `Record` revision derive; in-place readers decline these bytes and use the decoded row |
| `0xFF`, layout 0 | document | one value in the stored-value grammar | `ValueRef` in place, or a full decode |
| `0xFF`, layout 1, 2 or 3 | packed | one row of a generation's descriptor | the generation's descriptor |

The header's layout values 4 to 7 are refused. A later layout (`PAX`, `COLUMNAR`) is a new layout
value behind the feature level, with its own descriptor kind. Neither is built, and a table cannot
ask for either.

### 3.1 A packed row

```
header      FF, layout (1, 2 or 3 by end-offset width) and flags, generation (LEB128)
body        presence bits · bool bits · fixed slots | end offsets · variable region | trailer?
```

- **Offsets are the descriptor's.** Each column has a physical kind, a presence and a slot offset
  the generation's descriptor records; a reader finds a fixed column at `base + offset` with no
  parse. Variable-width values sit behind a per-row end-offset table of the narrowest width that
  holds the region.
- **`NONE` and `NULL` are distinct.** A column that can be absent has a presence bit, a column
  that can be `NULL` has one, and a column that can be both has two, so `option<T>`, `T | null` and
  `option<T | null>` round-trip. A `NONE` is never held in a column. A column that can be neither
  has no bit.
- **Types carry no tag where the schema fixes them.** An `int` is eight bytes. A kind the layout
  has no column for (`any`, `FLEXIBLE`, a union it cannot code) is a `Value` column holding a
  stored-grammar value.
- **A value that does not fit its slot escapes to a trailer.** A datetime or duration beyond what
  eight bytes hold holds a reserved extreme in its slot and the value in the trailer, so the slot
  still orders correctly against its own trailer value.
- **Declared objects are flattened.** An `object` field with defined children is a node with no
  slot, and each child is a column at its path (`address.city`); a `FLEXIBLE` object is one `Value`
  column. The row format also has a remainder column for the keys of an open node, which the
  derived descriptors do not yet use.
- **`id` is the key.** It is not stored in the value; every reader restores it from the record's
  key.

The row rules (`surrealdb/catalog/src/packed/mod.rs`) are permanent for 4.0 readers: a row not in
its canonical form is refused, and a later release that relaxes a rule adds a descriptor field that
raises the descriptor's read floor.

### 3.2 `PAX` and `COLUMNAR`

Not built. A `PAX` chunk would hold a run of records with each column contiguous; a `COLUMNAR`
value one column of a key range. Each trades read speed against write amplification, and each
would be a table-level choice made through a new layout value. The VM's batch instructions alias a
`PAX` chunk's columns as lanes (§7).

## 4. Layout generations

A table's layout is a sequence of generations. Generation 0 is the rows with no descriptor:
`revision` rows and documents. This is how every existing table enters the scheme without being
rewritten.

- A table's generations form a ring of the numbers 1 to 65535. Each has one immutable descriptor
  under `!lg{generation}` in the table's key range, written by the statement that mints it, naming
  its table.
- A row carries its generation in its header. A reader admits the generation against the table's
  descriptors before it reads the body and refuses, with a typed error, one outside the ring or
  without a descriptor.
- DDL mints generations: after every statement that changes a `SCHEMAFULL` table's fields, the
  table is stamped, and when the layout derived from its field definitions differs from its newest
  descriptor the stamp mints the next generation. A statement that changes no declared field, kind
  or optionality mints nothing.
- The table records its ring (floor, base and head) and a conformance record of the fields every
  row is known to conform to (§5.5). They guide planning only.
- Moving the floor, which is what retires a generation's readers, must first make sure no row and
  no transaction that could still write one carries an earlier number, because a number reused
  after the ring wraps would otherwise read a row through the wrong descriptor.

## 5. Migration

A migration is a step a generation carries. A row stamped before the generation passes through the
step whenever it is read, and a background job rewrites the rows so that reading them costs nothing
extra. The step is part of the generation's immutable descriptor, so what a row reads as never
depends on whether the job has reached it.

### 5.1 The statements

- `DEFINE FIELD` and `ALTER FIELD … MIGRATE EXISTING ROWS AS <name> USING <expr> [FALLBACK
  <literal>]` change a field's definition and say what each existing row's value becomes.
- `ALTER TABLE … MIGRATE EXISTING ROWS AS <name> REMOVE UNDECLARED FIELDS` removes, from every row
  stored before it, each top-level field the table did not declare when the step was minted.
- `REMOVE FIELD` and a field made `COMPUTED` on a table that enforces migrations (§5.2) mint a
  removal step for the field's stored values.
- A database has a compatibility level, `3` or `4` (`DEFINE DATABASE … COMPATIBILITY`, `ALTER
  DATABASE COMPATIBILITY`). The store has a default for the databases it creates (`ALTER SYSTEM
  COMPATIBILITY`): level 4 for a store this release creates, level 3 for one an earlier release
  created, recorded by a ledger migration. A database that records no level has level 3.

A migration's name is unique per table. It is recorded in a permanent registry (`!mn`) that
outlives the descriptor, so restating the statement after the generation is retired does not run
it again.

### 5.2 Compatibility levels

At level 3 a table accepts any schema change, as the 3.x releases did: a removed field keeps its
stored values and a retyped field leaves rows that do not conform. At level 4 a `SCHEMAFULL` table
refuses a schema change its stored rows might not satisfy unless the statement carries `MIGRATE
EXISTING ROWS` to say what they become, and a removed field's values are removed with a step.
Raising a database to level 4 verifies its tables (§5.5). A table with no rows takes any change at
either level.

A level-4 database, and any `MIGRATE` statement, is written in the stored grammar and so is refused
by a store at the Legacy feature level; the refusal names the feature level. A level-3 database is
written in `revision` and every 3.x release reads it.

### 5.3 What a transform may do

A transform is checked when the statement is compiled; anything unproven is an error then, never a
failure part-way through a backfill.

1. **Pure.** Its effect set is exactly `EffectSet::PURE`: no data or catalog read, no write, no
   clock, no entropy, no external call.
2. **Reads only `$value` and constants.** No other parameter, no field of the row, no subquery, no
   graph traversal, and no call whose body the transform does not hold (`fn::`, a model, a script,
   a module). A step through a value that may be a record link, which would fetch the record, is
   refused unless the kind of the value admits no link there.
3. **Evaluated under a frame that refuses writes and record dereference.** A dereference that
   slips the check raises a value error, which takes the `FALLBACK`.
4. **Tested.** The `FALLBACK` is checked against the field's new kind when the migration is
   defined, and a result is judged against the same test (the new kind, `SCHEMAFULL`/`FLEXIBLE` and
   an `ASSERT` over `$value`) when the step runs. A result that fails it takes the `FALLBACK`.
   A result passes the new kind only through a coercion that loses nothing: a number, at any
   depth, may change representation only where it converts back to the value it was, and a set
   must hold apart every element the result held apart, so the duplicates of an array are the
   only elements a set merges.

Only an error that depends on the value alone (a failed cast or coercion, an operator given
operands of the wrong type, a builtin refusing its argument) takes the `FALLBACK`. A timeout,
cancellation, memory or size limit, capability denial or storage failure propagates, so no node
stores a fallback for a row another node converts. A step with no `FALLBACK` that meets a value it
cannot migrate fails the read of that row.

### 5.4 How rows move

- **On read.** A record stored before a step's generation is brought through every step it has not
  passed, oldest first, where it becomes a value of the engine: in a scan of the table's range, in
  a read by id, and where a write adopts its pre-image. Replay never runs in the record decoder, so
  the transaction's record cache holds records as stored with the generation they carry. A row with
  no generation is before every step. The head generation's descriptor names the newest generation
  that carries a step (`LayoutDescriptor::migrated`), so a scan of a table whose rows are all at or
  past it does no per-row work beyond comparing the stamp.
- **On write.** A write stamps the row with the table's head generation, so only rows stored before
  the newest step ever need replay.
- **In the background.** One lease (`TaskLeaseType::TableMigration`) covers the job for every
  table. A table with work has one queue entry at `!tm` (`TableMigrationState`) holding a cursor
  and the counts of the lap. The job reads a page of records from its own snapshot, brings each
  through the steps it has not passed and stores it in the layout of the head generation, then
  advances the entry by compare-and-set on the state it read. A page whose entry was replaced or
  deleted meanwhile commits nothing. A lap ends when the table's newest step is the one the lap
  began under; otherwise another lap begins. Any node resumes from the stored cursor.
- **No retirement.** The job does not move the ring's floor. Moving it retires descriptors and
  erases the `VERSION` history of the generations it passes, so it is an operator decision rather
  than a side effect of a backfill. The steps stay in the ring, and a straggler row, one a
  transaction that began before the migration committed after the lap passed it, is replayed
  whenever it is read.

A transform is deterministic, so a record read through it and the same record after the job are
identical; that equality is what the replay tests and the upgrade tests assert (§8).

### 5.5 Verification and claims

A field whose definition may leave stored values that do not conform (a new field, or one whose
`TYPE`, `FLEXIBLE` or `ASSERT` changed) is queued for verification: the job judges each record's
value, brought through any pending step, without writing it, against the test a migration step of
the field would apply. That is the field's type with the definitions beneath it folded in, so a key
no definition declares beneath an object of a `SCHEMAFULL` table fails it, and its `ASSERT` over
`$value`. A field every value of which conformed gets a claim in the table's conformance record
(`!fc`): every row stamped with a generation of the ring holds, at every occupied path of the field,
a value the claimed definition coerces. The claim records the digest of the definition it was
settled against, so any change of the definition ends it. Any change of a table's field
definitions, and a change that makes the table `SCHEMAFULL`, forfeits the table's claims; a pass
under way starts over when the table becomes `SCHEMAFULL` or a definition beneath a field changes,
since either changes what the pass judges a field against.

A claim is made only when all of these hold: no value failed, the test was the field's whole
definition (it is not where the `ASSERT` reads more than `$value`, or a definition beneath the
field has a type the fold cannot carry), the lap began after every transaction that could still
commit an unjudged row had ended (the datastore is the store's sole writer and the job waited for
the transactions older than the change; any other store reports counts and makes no claim), the
rewrite is done, the field is one the conformance record can state, and the store is not at the
Legacy feature level. An absent value is judged as `NONE`. A pass that finds a value that does not
conform withdraws the table's claims when one is on the definition it judged. A write the schema
stage does not judge (`OPTION IMPORT`) forfeits the table's claims and restarts the verification.
The failures found are shown in `INFO FOR TABLE`, with the migrations the table has run, the fields
it guarantees and the progress of a lap under way.

A table that comes to enforce migrations over rows (a `SCHEMALESS` table made `SCHEMAFULL` at level
4, or a database raised to level 4) is also checked for rows holding a top-level key no definition
declares. Level 4 keeps such a key until `REMOVE UNDECLARED FIELDS` clears it, but a `SCHEMAFULL`
table refuses every write of the row, so the check reports the rows a later write refuses. It
judges each record, brought through any pending step, by its top-level keys: one other than `id`,
`in` and `out` that no definition's path begins with (the names `REMOVE UNDECLARED FIELDS` keeps)
counts the row once. It states no claim. A lap that judged every row and counted none leaves
nothing to report; otherwise `INFO FOR TABLE` shows `undeclared: { failures, finished }`. The check
runs again, finished or not, when the names the table declares at the top level change or `REMOVE
UNDECLARED FIELDS` clears keys from the rows, and a lap that has passed rows under names that no
longer stand starts over. A table made `SCHEMALESS` drops the check, since nothing refuses such a
key there.

A claim lets a migration leave out `FALLBACK` only where its transform is `$value` itself, or a
cast of it to a number kind, and the previous definition's test, folded as the claim vouches for
it, widens to the step's test.

### 5.6 Derived data

An index, a `COUNT` index, a full-text index or a table view reads field values. An index on a
field a pending removal step still clears is refused until the job has rewritten the rows, so an
index never describes values a read no longer sees. A table view reads the replayed values.

### 5.7 What a migration does not do

A record read through a step has not been written: no event, live query or changefeed fires for
it, before or after the job. A migration changes representation, not data.

## 6. Plan changes

- **Logical.** A migration is a schema statement (`LogicalOp::Ddl`) carrying its clause whole. The
  transform is compiled when the statement runs, against the table's definitions as they stand.
- **Physical.** The statement runs as a lowered program of store instructions
  (`vm/store.rs`) in one transaction: it mints the generation that carries the step, stores the
  field definition, and queues the rewrite and the verification. A statement run by a route that
  resolves the definition before it runs refuses `MIGRATE` rather than run it late-bound.
- **Reads.** A scan, a read by id and a write's pre-image take the table's replay (`Replay`) when
  its head generation says a step may be pending. Per-row stamps travel with the batch
  (`ValueBatch::stamped`). A row stored before the newest step is judged only after replay, so the
  pre-decode filter and the top-k probe skip it, and an in-place scan decodes it.
- **Programs declare what they do.** The step is stored as the text of its transform with the test
  its result must pass, and is compiled once per `(namespace, database, table, generation)`; the
  compiled step is validated against the stored step by equality before it is used.

## 7. How the VM reads each layout

- **`RowReader`** answers a field of one row as a borrowed `Scalar`. `StoredRow` reads a `revision`
  record by walking its encoded map and a document by a search of the stored object's sorted keys,
  probing first the slot the field held in the last row it was found in.
- **A packed row** is decoded whole through its generation's descriptor and read as a value: the
  VM declines the in-place readers for it, so no packed bytes reach a sort, a frame or a spill. The
  packed reader itself (`PackedRow::field_with`, `FieldPlan`) reads a field without decoding the
  rest of the row, and resolving a field to its slot once per scan and generation is how the VM
  will read a packed row in place.
- **`RowBatch`** (proposed) answers a field for N rows as lanes plus a per-lane kind: a slice of a
  `PAX` column as it lies, a column of a `COLUMNAR` range, or values gathered from packed rows into
  an aligned buffer. A row program has no effects, so evaluating its filter over a batch at once
  answers what evaluating it row by row answers; lanes of an unexpected kind take the scalar path.

## 8. Retiring `revision`

1. 4.0 reads both encodings on every store and writes `revision` while the store is at Legacy, so
   a cluster that has not finished upgrading never has an older node meet a record it cannot read.
2. A store writes the new encoding once its feature level is raised: a single-writer engine
   raises itself at startup, and any other engine waits for an operator, who raises it once no node
   older than 4.0 remains (`RFC_RECORD_FORMAT_ROLLOUT.md`).
3. A table's old rows converge as writes touch them, and a migration's job (§5.4) moves the rest.
4. A later major release drops the `revision` record reader once the minimum supported upgrade path
   starts above 3.x and no generation 0 `revision` record remains.

Catalog entries have their own schedule: they are keyed maps in the stored grammar, read floors
and a feature level govern them, and keys use `storekey` and are unaffected. Exports are SurrealQL
text.

Every step adds upgrade pairs to `language-tests/tests/upgrade/` (written by the older release,
read by the newer) as `CLAUDE.md` requires, and a pair per migration shape asserting that a record
read through its transform equals the record after the backfill.

## 9. Open questions

1. How `PAX` chunks are assigned: by key range, fixed count, or byte budget, and whether `COLUMNAR`
   follows `PAX`.
2. Pausing and cancelling a migration in progress.
3. Moving the ring's floor once a table's rows are all at the head, and what it does to `VERSION`
   reads of the generations it retires.
4. A totality check of a transform over the kinds it receives, which would make `FALLBACK` optional
   for a transform proven total.
