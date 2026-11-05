# ADR 0009: The 4.0 record format

**Status:** Accepted · **Scope:** `surrealdb/expr/src/val/stored/`, `surrealdb/catalog/src/`
(`record.rs`, `layout.rs`, `layout_derive.rs`, `generation.rs`, `conformance.rs`, `packed/`,
`feature.rs`, `stored/`), `surrealdb/datastore/src/` (`tx.rs`, `layouts.rs`, `factory.rs`,
`feature.rs`), `surrealdb/core/src/kvs/` (`ds.rs`, `migration/`, `rollback.rs`),
`surrealdb/core/src/exec/vm/`

**Relates to:** ADR 0007 (the VM reads rows through `RowReader`), ADR 0008 (named record-id
components, which a packed row leaves in the key), ADR 0010 (in-place reads of a record agree with
its whole decode). Operator-facing behaviour is in
`doc/UPGRADING.md`; the layouts and the migration statements are in
`doc/executor/RFC_RECORD_LAYOUT_AND_MIGRATE.md`; how a store reaches the format is in
`doc/executor/RFC_RECORD_FORMAT_ROLLOUT.md`.

## Context

Every release before 4.0 stores a record as a `revision`-encoded `Record`: a self-describing tree
with a tag on every value. Reading one field walks the encoded object and decodes the field
through the generic deserializer, which costs more than the storage cursor that produced the row.

Whatever byte grammar 4.0 writes, every later release must decode, and every build, the
WebAssembly one included, ships that decoder. So the format has to be few grammars, each one
right, and every row has to say which grammar it is in, so that a later layout is a new value and
not a change to an old one.

A fixed-slot ("strict") layout was proposed: every field of a `SCHEMAFULL` table at an offset its
generation assigns, a two-bit null map for every column, a 16-byte view for every string, and an
8-byte fingerprint on every row naming its descriptor. Measured on six row shapes against the
stored-value grammar and a packed row, its rows were larger than a packed row's on five of them (a
16-byte envelope; 16 bytes for a two-letter string; a null-map entry for a column that can never be
`NONE`), and its fingerprint was resolved on every row through a process-wide lock. A field read
by name was an order of magnitude slower than a packed row's constant-offset read.

## Decision

**One envelope, dispatched on the first byte, per row, forever.** A record that starts with `0xFF`
is in the v1 envelope: `FF`, a header byte, the generation as canonical LEB128, then the body. Any
other first byte is a `revision` record, which no 3.x release writes with `0xFF`, so a store holds
both side by side and a pre-4.0 build fails on the first byte. The header names the layout and
carries the edge flag; bits it does not define are refused, so a later release that sets one has to
raise a read floor first. `catalog::StoredRecord` holds the byte-level contract.

**Two body layouts.** The *document* layout is one value in the stored-value grammar
(`surrealdb_expr::val::stored`): tagged, offset-indexed containers whose objects keep their keys
sorted, so a field is found by binary search and an array element by index, read in place through
`ValueRef` without allocating. A record whose `id` is its key leaves the `id` out. The *packed*
layout is the row of a `SCHEMAFULL` table: fixed-width values at constant offsets, variable-width
values behind a per-row end-offset table, presence bits only for `option` and nullable columns, no
fingerprint and no `id`. A value a column cannot hold (another type under `OPTION IMPORT`, a key no
field declares, a leap second) makes the writer fall back to a document at the same generation, so
a table's rows can be any mix and every read answers one value whichever it meets.

**Generations name descriptors.** A table's rows carry a generation, a number in a ring of 1 to
65535 (0 means no descriptor, and holds documents). Each non-zero generation has one immutable
descriptor under `!lg{generation}` in the table's key range, written create-only by the statement
that mints it and naming its table. A per-name high-water mark (`!gn`) makes a number name one
descriptor for the life of a table name, which is what lets the row carry no fingerprint. A reader
admits a row's generation against the table's descriptors before it reads the body; a generation
outside the ring or without a descriptor is a typed refusal. The table also records a floor, the
oldest generation any row may carry, which guides planning and is never trusted for correctness,
and a record of the fields every row is known to conform to (`!fc`).

**DDL mints the layout.** A `SCHEMAFULL` table that is not a view, `DROP` or a lightweight relation
derives its descriptor from its field definitions. Each DDL statement ends by stamping the table:
when the derived layout differs from the newest descriptor, the statement mints the next
generation in its own transaction; otherwise it mints nothing. The derivation is a function of the
catalog alone, so two nodes of one release derive the same bytes. There is no layout opt-in or
opt-out in 4.0.

**Descriptor and row rules are permanent for 4.0 readers.** A descriptor decodes only to a
canonical form whose every offset lies inside its declared sizes, so a reader that follows a
validated descriptor never reads a wrong offset. A row is refused, not repaired, when it is not in
its canonical form (the narrowest end-offset width, no padding bits, no trailing bytes, an
escape trailer that agrees with its slots). A later release that writes what one of those rules
refuses adds a descriptor field that raises the descriptor's read floor, so 4.0 refuses the whole
generation at admission instead of failing row by row.

**The catalog is in the same grammar.** Catalog entries are keyed maps declared once per type in a
schema table (`catalog-schema.lock`), each field with a permanent number and a class, each entry
with a read floor. A release reads an entry whose floor it meets and refuses, by name and without
guessing, one it does not.

**A store records the newest release its encodings may use (its feature level).** Below it, a
store is at Legacy: every record and entry is `revision`. A single-writer engine raises itself to
4.0 on startup; other engines wait for an operator's call; `legacy` holds a store where it is. The
migration ledger carries the format as an incompatible entry (id 7) committed in the transaction
that writes the first byte that needs it, so a build that does not know the format is refused
before it reads one. Every node publishes its release beside its heartbeat, and a raise is refused
while a live node runs an older one. Ledger ids carry a class in their top bits (compatible,
read-only compatible, incompatible, reserved), and read-only compatible entries open a store
read-only.

**Reserved room.** Header layout values 4 to 7 are refused today. A later layout (PAX, columnar) is
a new layout value behind the feature level, with its own descriptor kind; nothing in 4.0 has to
change for it.

## Consequences

- A table upgraded from 3.x is mixed: its rows are `revision` at generation 0 and read through the
  general path, at the speed they read today. New writes are documents until the table's first
  stamp mints a generation, and packed after that. The rows converge when something rewrites them;
  an ordinary write re-encodes the row it touches. `MIGRATE EXISTING ROWS` mints a generation
  whose descriptor carries the step that brings an older row to its value, and a background job
  rewrites the rows; the job does not move the floor, so a row stamped before a step is replayed
  whenever it is read (`doc/executor/RFC_RECORD_LAYOUT_AND_MIGRATE.md` §5).
- Rolling back to 3.x after the store is raised is refused. Rolling back within 4.x is supported
  until something uses a feature the earlier release lacks, and `Datastore::rollback_check` reports
  what an earlier release would refuse.
- The ring is 65535 stamps per table name. A statement that stamps a table whose ring is full is
  refused with `GenerationRingFull`, and every row stays readable. Moving a floor must first make
  sure no row, and no transaction that could still write one, carries a number before it, because
  a number reused after the ring wraps would otherwise read a row through the wrong descriptor.
- A descriptor or row is read in one canonical form, so bytes outside it are a refusal and never a
  different value. A packed row reaches an in-place reader only checked by its decode run in check
  mode (ADR 0010). The writer writes one form
  per record. The frozen fixtures pin the bytes: the envelope and documents in
  `surrealdb/core/src/kvs/compat/stored_record.rs`, packed rows and the descriptors they are read
  against in `surrealdb/catalog/src/packed/tests/fixtures.rs`. The fuzz targets
  (`fuzz/fuzz_targets/`, packed rows and descriptors in `fuzz_packed_row.rs`) and the seeded
  round-trip and mutation tests in `surrealdb/catalog/src/packed/tests.rs` hold the readers to
  it.
- A packed row has no `id` and no stored view or edge state beyond its flag, so view statistics
  and adjacency layouts live outside the row.

## Alternatives rejected

- **A strict fixed-slot layout with a descriptor fingerprint.** Larger than a packed row on most
  shapes, slower to read by name, a per-row registry lookup, and opt-in, so most tables never
  reached it. The packed row keeps its constant-offset reads and drops the per-column null map, the
  16-byte string view and the fingerprint.
- **A per-table layout switch.** A table's layout is derived from its field definitions, so the
  schema is the only input and two nodes cannot disagree. A switch would have made correctness
  depend on a setting.
- **Protobuf catalog entries.** The stored-value grammar already has a reader the format needs; a
  second grammar for the catalog would have doubled what every release must decode.
- **Rewriting a table on upgrade.** It is hours of I/O on a large store; reads are correct without
  it, and the statements that perform it are opt-in.
