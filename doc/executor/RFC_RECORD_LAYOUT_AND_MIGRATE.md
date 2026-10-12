# RFC: Record layouts, and migration

**Status:** the layouts and generations of §§1–4 and 7–8 are built (ADR 0009). The statements that
move rows between generations (§§5–6) are proposed.

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
- The table records its floor, the oldest generation any of its rows may carry (0 when unknown),
  and a conformance record of the fields every row is known to conform to. They guide planning only.
- Moving the floor, which is what retires a generation's readers, must first make sure no row and
  no transaction that could still write one carries an earlier number, because a number reused
  after the ring wraps would otherwise read a row through the wrong descriptor.

## 5. Migration

**Not built.** Two statements are planned to move rows between generations: `ALTER TABLE …
REWRITE`, which re-encodes every row below the newest generation as it stands (a step with no
transform), and `MIGRATE EXISTING ROWS … USING`, which applies a transform to each row. A
`DEFINE FIELD` is never refused for want of either: DDL mints a generation whether or not the rows
have moved. The
rules below decide which transform is acceptable. They are stated for a transform from generation
G to G+1.

### 5.1 What makes a transform acceptable

A transform is accepted only when all of these are proven when `MIGRATE` is
compiled; anything unproven is an error then, never a failure part-way
through a backfill.

1. **Pure.** `effects_of` over the body answers `EffectSet::PURE`: no data or catalog read, no
   write, no clock, no entropy, no external call, no opaque body.
2. **Reads only `$row` and constants.** No parameter, no record dereference, no subquery, no
   lookup.
3. **Infallible.** No node may raise for any input of the kinds it is proven to receive (§5.2).
   `node_may_raise` (`expr/src/expr/logical/effects.rs`) is the starting classification; §5.2 is
   what it needs to become kind-aware.
4. **Typed.** Each field of the result has a kind, derived from G's field kinds, equal to the kind
   G+1 declares for it, and the result has no field G+1 does not declare.

### 5.2 Infallibility is decided per node

Every node of the transform is judged, not only its calls: an expression
raises through a cast, an arithmetic operator, a dereference or a call alike.
The checker walks the body with the kind of every subexpression known —
`$row`'s fields have G's kinds, literals have theirs — and accepts a node only
when it cannot raise for any input of those kinds.

| node | accepted when | notes |
|---|---|---|
| literal, `$row`, a field of `$row` | always | a field absent from a G row reads `NONE`, which G's kind for it admits |
| object, array and set constructors, `IF`, `??`, `?:` | every operand is accepted | |
| comparisons, `AND`, `OR`, `NOT` | every operand is accepted | total for every pair of kinds (`operate`) |
| cast `<K> e` | the conversion from `e`'s proven kind to `K` is total | see below |
| arithmetic, unary `-` | never, in the first version | integer overflow raises; saturating forms may follow |
| field access through a value that may hold a record id | never | it would fetch the record; a field of record kind may be copied, not dereferenced |
| call | the builtin is declared total over the proven kinds of its arguments | see below |
| `THROW`, subqueries, lookups, closures, methods, parameters other than `$row` | never | |

**Casts are total or not by the pair of kinds.** `<record> e` raises for most
values, but not for a value already proven a record id; `<float> e` is total
from `int`; `<int> e` is not total from `string`. The checker holds a table of
conversions that cannot fail — identity, the numeric widenings, and whichever
conversions to `string` hold for every value of their source kind — and every
other cast is refused, however likely it is to succeed on the data at hand.
Each entry of the table is verified by the same property test that verifies a
builtin's totality. Where a conversion can fail, the transform
uses a total form that answers `NONE` instead of raising — `type::try_int`,
`type::try_record`, … — with `??` supplying the value.

**A builtin states its totality.** No builtin declares today whether it can
raise, so `node_may_raise` treats every call as raising. Each builtin declares
whether it is total over the kinds it accepts, in the `define_*_function!`
macros, required and default-partial like `EffectSet`. A declaration of
totality is checked by a property test over generated inputs of every kind the
builtin accepts: it is a claim the suite verifies, not one the checker trusts.

**The result is exactly G+1's shape.** Assigning a value to a field of a
declared type coerces it, and coercion raises. So the result's kinds must
equal G+1's declared kinds (§5.1, rule 4) and no coercion ever runs: the
transform, not the field definition, is where every conversion is written.

### 5.3 How rows move to G+1

- **On read.** A reader meeting a G record applies the transform. The transform is a pure,
  infallible native program, so it runs inside the row program as a prelude; a transform that only
  renames, defaults and drops fields reduces to a field mapping in the reader.
- **On write.** A writer touching a G record reads it through the transform, applies its change,
  and writes G+1.
- **In the background.** A backfill program rewrites cold ranges in batches, each record with a
  conditional put — only while it is still G — so it never overwrites a concurrent writer. Its
  progress is the last key it committed; any node resumes it from there
  ([`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md)).
- **Retirement.** When a scan finds no G record, the transform is dropped and the table's floor
  moves past G (ADR 0009: the floor is recorded only once no row, and no transaction that could
  still write one, carries an earlier generation).

A transform is deterministic, so a record read through it and the same record
after the backfill are identical; that equality is what the upgrade tests
assert (§8).

### 5.4 Derived data

An index, a `COUNT` index, a full-text index or a table view reads field
values. A transform that changes the value of a field one of them reads would
leave it describing G while reads describe G+1. The first version refuses such
a transform. A later one rebuilds the affected structures, concurrently,
before G+1 becomes visible.

### 5.5 What a migration does not do

A record read through a transform has not been written: no event, live query
or changefeed fires for it, before or after the backfill. A migration changes
representation, not data.

## 6. Plan changes (proposed)

- **Logical.** A migration lowers to its own `LogicalOp`. Binding resolves the table's current
  generation and schema, which the transform is checked against (§5.1): the check is
  catalog-bound, so it runs at bind, not at lowering.
- **Physical.** The plan commits generation G+1 (the definitions and the layout derived from
  them), stores the transform as a program, and triggers the backfill program, all in one
  transaction.
- **Reads.** Mapping a scan of a table in migration records both generations as dependencies, and
  the row program gets the transform as a prelude for G records. Which generation a record is in is
  read from its header, per row.
- **Programs declare what they do.** The transform is stored as bytecode, and its acceptance is a
  property of that bytecode, not of the SurrealQL it came from: a program declares its effects,
  whether it writes, and whether it can fail, derived from its instructions
  ([`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md) §3).

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
3. A table's old rows converge as writes touch them, and a rewrite (§5) moves the rest.
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
2. The surface of a migration in progress: `INFO FOR TABLE` progress, pausing, cancelling, and what
   a statement answers while a backfill runs.
3. Whether turning on a table's required migrations queues its rewrite.
