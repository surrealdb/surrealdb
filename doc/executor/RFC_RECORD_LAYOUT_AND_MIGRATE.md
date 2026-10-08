# RFC: Record layouts, and `MIGRATE`

**Status:** draft. Proposes a stored record format with row, PAX and columnar
layouts, the `MIGRATE` statement that is the only way to change a strict
table's layout, and how `revision`-encoded records retire under it.

Companion: [`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md),
on which a migration's backfill runs as a triggered program. Requirements this builds on:
[`PHYSICAL_PLAN_REQUIREMENTS.md`](PHYSICAL_PLAN_REQUIREMENTS.md).

---

## 1. Why

A record today is one KV value holding a `revision`-encoded `Record`: a
self-describing tree with a tag on every value. Reading one field means
walking the encoded object and decoding the field through the generic
deserializer; the `where_the_time_goes` probe in `core/src/exec/vm/tests.rs`
shows that locating and decoding one integer costs more than the storage
cursor that produced the row. Nothing about a record's shape is known before
it is read, so no field has a known offset, no batch of rows has a column to
load, and every read pays for generality the schema already rules out.

The VM reads rows only through `RowReader` (`core/src/exec/vm/row.rs`), so a
new encoding is a new implementation behind that interface, not a change to
the VM.

## 2. Principles

1. **One logical record, several physical layouts.** A table's rows mean the
   same whatever layout stores them; the layout is a performance choice.
2. **Strict is opt-in, per table.** A schemaless table keeps a tagged,
   self-describing encoding and never needs `MIGRATE`. Existing users see no
   change until they choose a strict layout.
3. **A strict layout is a guarantee, not a hint.** Every row of layout
   generation G has every field of G at the offset G assigns it, of the type G
   declares. Nothing that could break that is accepted outside `MIGRATE`.
4. **A migration never fails and never writes on its own behalf.** Its
   transform is proven pure and total before it is accepted (§5), which is
   what lets a record be migrated lazily, on read, any number of times, with
   the same answer the backfill writes.

## 3. Layouts

`DEFINE TABLE … SCHEMAFULL LAYOUT ROW | PAX | COLUMNAR`. The default for a
strict table is `ROW`; a table without `LAYOUT` keeps the tagged encoding.

| layout | a KV value holds | reads are cheap for | writes cost |
|---|---|---|---|
| `ROW` | one record | point reads, whole-row projection | one value per record |
| `PAX` | a chunk of up to N records, each column's values contiguous within the chunk | filtered scans, aggregates, SIMD over a column | rewriting the chunk a record is in |
| `COLUMNAR` | one column of a key range | analytics over few columns | rewriting every column the write touches |

The choice trades read speed against write amplification, and the storage
engine's merge absorbs part of the write cost. It is a per-table decision with
a default, not a global one.

### 3.1 A record in a strict layout

```
header      layout generation id (varint) · flags
null map    two bits per nullable field: present, null
fixed       every field's slot, at an offset the generation assigns, aligned
var region  the bytes of variable-width values that do not fit their slot
tagged      FLEXIBLE and `any` fields: tagged values, as today
```

- **Offsets are the layout planner's, not declaration order.** Fields are placed by alignment to
  minimise padding; the generation records the result. A reader finds every field's slot at
  `base + offset`, with no parse.
- **`NONE` and `NULL` are distinct.** A field typed `option<T>` can be absent; one typed
  `option<T | null>` can also be `NULL`. Two bits per nullable field — or, in a `PAX` chunk, two
  bitmaps per column — hold both, which is also the lane mask a vectorised compare consumes.
- **Types carry no tag where the schema fixes them.** A field declared `int` is eight bytes. Only
  the tagged region pays for self-description.
- **A string is a view.** Its slot holds its length and, when the string fits, the whole string;
  otherwise a prefix and an offset into the variable region. Every string field therefore has a
  fixed offset, and equality and ordering are decided from the slot — the length and the inline
  bytes — for most pairs, reaching the variable region only when the prefixes agree. The default
  slot is 16 bytes (12 inline). A generation may choose a different inline width for a column from
  its lengths' distribution; because the bytes depend on it, the width is part of the generation
  and changes only through a re-layout (§5), which keeps every value and cannot fail.
- **A view says whether its string is ASCII.** The high bit of the length word is set when every
  byte is ASCII, which the encoder learns while copying the string. `string::len` counts
  characters, so for an ASCII string it is the length word, read from the slot without touching the
  string; any other string counts its characters.
- **`id` is the key.** It is not stored in the value, as today.

### 3.2 A `PAX` chunk

The chunk's header names its generation and row count; each column follows as
a contiguous run (fixed-width values, or offsets into a shared variable
region), with the column's null bitmaps. A field of row *i* is
`column_base + i * width`. This is the "batched row" the VM's batch
instructions alias (§7).

## 4. Layout generations

A table's layout is a sequence of generations. Generation 0 of every existing
table is the `revision` encoding, which is how existing data enters the scheme
without being rewritten first.

- The generation id is stored in every record header and, for a table, in its catalog definition,
  separate from the rest of the definition so that unrelated DDL does not change it.
- A cached physical plan is keyed by the generation it was lowered against (ADR 0003): a program
  holds the offsets of one generation.
- A table has at most one generation in migration at a time. `MIGRATE` is refused while rows of an
  older generation remain.

## 5. `MIGRATE`

```surql
MIGRATE person {
    DEFINE FIELD age  TYPE int;
    DEFINE FIELD tags TYPE array<string>;
    REMOVE FIELD legacy_age;
} WITH |$row| {
    { ...$row, age: type::try_int($row.legacy_age) ?? 0, tags: $row.tags ?? [] }
};
```

On a strict table, a `DEFINE FIELD` or `REMOVE FIELD` that changes the layout
is refused outside `MIGRATE`. Inside one, the definitions take effect together
as generation G+1, and the `WITH` closure maps a G row to a G+1 row.

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
- **Retirement.** When a scan finds no G record, the transform and G's reader are dropped.

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

## 6. Plan changes

- **Logical.** `LogicalOp::Migrate { table, definitions, transform }`, lowered from the statement.
  Binding resolves the table's current generation and schema, which the transform is checked
  against (§5.1): the check is catalog-bound, so it runs at bind, not at lowering.
- **Physical.** `Plan::Migrate` commits generation G+1 (the definitions and the layout the planner
  assigns), stores the transform as a program, and triggers the backfill program — all
  in one transaction.
- **Reads.** Mapping a scan of a table in migration records both generations as dependencies, and
  the row program gets the transform as a prelude for G records. Which generation a record is in is
  read from its header, per row.
- **Programs declare what they do.** The transform is stored as bytecode, and its acceptance is a
  property of that bytecode, not of the SurrealQL it came from: a program declares its effects,
  whether it writes, and whether it can fail, derived from its instructions
  ([`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md) §3).

## 7. How the VM reads each layout

- **`RowReader`** answers a field of one row as a borrowed `Scalar`. `ROW` reads it at
  `base + offset`; the tagged encoding keeps today's lookup.
- **`RowBatch`** answers a field for N rows as lanes plus a per-lane kind: a slice of a `PAX`
  column as it lies, a column of a `COLUMNAR` range, or values gathered from `ROW` records into an
  aligned buffer. A row program has no effects, so evaluating its filter over a batch at once
  answers what evaluating it row by row answers; lanes of an unexpected kind take the scalar path.

## 8. Retiring `revision`

1. Release N reads both encodings and writes `revision`, so a rolling upgrade never has an old
   node meet a record it cannot read.
2. Release N+1 writes the new encoding, once the cluster's storage version (`kvs/version`) says no
   node older than N remains.
3. A ledgered migration (`kvs/migration`) backfills generation 0 records as §5.3 describes.
4. A later major release drops the `revision` record reader once the minimum supported upgrade path
   starts above N and no generation 0 record remains.

Catalog definitions and changefeeds are separate encodings with their own
schedule; keys use `storekey` and are unaffected. Exports are SurrealQL text.

Every step adds upgrade pairs to `language-tests/tests/upgrade/` — written by
the older release, read by the newer — as `CLAUDE.md` requires, and a pair per
migration shape asserting that a record read through its transform equals the
record after the backfill.

## 9. Open questions

1. `PAX` chunk size and how records are assigned to chunks: by key range, fixed count, or byte
   budget.
2. Whether `COLUMNAR` belongs in the first version, or follows `ROW` and `PAX`.
3. Whether a strict table may hold a tagged region at all, or requires every field typed.
4. The user-visible surface of a migration in progress: `INFO FOR TABLE` progress, cancellation,
   and what `MIGRATE` answers while a backfill runs.
5. Who owns the storage format change, and the release that carries step 1 of §8.
