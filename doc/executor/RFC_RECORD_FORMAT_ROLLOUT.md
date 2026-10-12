# RFC: Rolling out the record format

**Status:** built, except where a section says it is proposed (§6). How a store reaches the 4.0
record format of [`RFC_RECORD_LAYOUT_AND_MIGRATE.md`](RFC_RECORD_LAYOUT_AND_MIGRATE.md): what it
represents, how it coexists with the `revision` encoding, when a store may write it, which tables
get packed rows, and how DDL behaves on a table that has them. The decision record is
[`../adr/0009-the-4-0-record-format.md`](../adr/0009-the-4-0-record-format.md); the operator's view
is the 4.0 section of [`../UPGRADING.md`](../UPGRADING.md).

---

## 1. Why

Every VM optimisation that needs a field's kind to be known (typed instructions with no per-row
check, a builtin answered from a value's slot, a vectorised region over a typed column) needs the
field stored untagged at a known offset. A packed row stores it there. Until stored data uses the
format, none of that applies, and the format cannot be introduced without a way for a store to
know that every node can read it.

## 2. What exists

| mechanism | behaviour | where |
|---|---|---|
| major version `!v` | a u16; a store created above Legacy, or raised, carries 4, and 3.0 to 3.2 refuse it | `datastore/src/version/mod.rs`, `core/src/kvs/ds.rs` `check_version` |
| storage stamp `/!vs` | the semver of the newest build that started; the version history records downward notes when an earlier writer touched the store | `core/src/kvs/migration/mod.rs` |
| migration ledger `/!mg{id}` | a registered migration runs once under a lease; an unknown entry refuses the store or opens it read-only, by the class in its id | `core/src/kvs/migration/mod.rs` |
| feature level `!fl` | the newest release whose encodings the store may use, and the newest in use | `catalog/src/feature.rs`, `core/src/kvs/ds.rs` |
| node release `!nr{node}` | the release a node runs, written with its heartbeat | `catalog/src/node.rs`, `datastore/src/tx.rs` |
| layout descriptors `!lg{generation}`, marks `!gn` | per table: the immutable descriptor of each generation and the name's high-water mark | `catalog/src/layout.rs`, `catalog/src/generation.rs` |
| record encoding | `revision` `Record`, or the v1 envelope (`0xFF`) | `catalog/src/record.rs` |

## 3. Representation

Every record a table can hold is representable and round-trips exactly:

| a field declared | is stored in a packed row as |
|---|---|
| `bool` | a bit |
| `int`, `float`, `uuid` | a fixed slot of the kind's width |
| `decimal`, `string`, `bytes` | a variable slice behind the end-offset table |
| `datetime`, `duration` | an 8-byte slot, with a trailer entry for a value beyond it |
| `record<t>` | a key column naming the tables it may hold |
| a literal union of strings, numbers, durations or bools | a code |
| `geometry<point>` | a point slot |
| `option<T>`, `T \| null` | `T`'s column with a presence bit (two for both) |
| an `object` with defined children | no slot of its own: each child is a column at its path |
| `any`, `FLEXIBLE`, an array, a set, other geometry, a union the layout cannot code | a `Value` column: one stored-grammar value |

A record the descriptor cannot hold is written as a document at the same generation. Every row
`pack` returns has passed `check_packed`, which checks its framing against the descriptor, and a
debug build reads each row the record writer encodes back and compares it with the record.

## 4. Coexistence and when a store may write

**Reading both is unconditional.** A record that starts with `0xFF` is in the envelope and any other
first byte is `revision`; no `revision` record starts with `0xFF`. A release that can read the
format reads it everywhere, with no flag.

**Writing needs every node to read it.** An older node meeting a v1 record fails to decode it, so a
store writes the format only when no node older than its first reading release can still write.
The store records that as a feature level, and every node reads the level it writes at:

| engine | how the level rises |
|---|---|
| memory, RocksDB, SurrealKV | the engine reports that one process owns the store; `check_version` raises a store at Legacy to 4.0 at startup |
| TiKV, IndxDB | the operator calls `Datastore::raise_feature_level`; it is refused while a live node records an older release, or none |
| any | `SURREAL_FEATURE_LEVEL` (or `Builder::with_feature_level`) names a release the store is created at or raised to at least; `legacy` holds a store at Legacy |

Once any v1 record or stored-grammar catalog entry exists, the ledger holds the incompatible entry
that records it, committed in the transaction that writes the first such byte, so a build that
cannot read them is refused at startup before it reads one.

## 5. Which tables, and how existing ones move

- **A new `SCHEMAFULL` table** is born with a generation: the first stamp of the table mints one
  and records its floor, so no row of the table predates it. Its rows are packed when every value
  fits and documents otherwise. Every other table writes documents.
- **An existing table** is mixed. Its rows are `revision` at generation 0 and read through the
  general path; new writes are documents until a DDL statement stamps the table and mints its
  first generation, and packed after that. An ordinary write re-encodes the row it touches at the
  table's current format. Nothing rewrites the rest unless an operator asks (§6).
- **Rows that do not conform.** A `DEFINE FIELD` never checked existing rows, so a table can hold a
  value its schema rejects. Reading is unaffected: the row stays a document or `revision` and
  reads as it always did. The conformance record of the table drops the field, so the planner does
  not compile a typed scan over it.

## 6. DDL on a table, and the proposed rewrite

A change to a `SCHEMAFULL` table's fields is a new generation when it changes the derived layout:
adding a field, widening to `option<T>` and removing a field each mint one, and a statement that
changes none of a field's declared kind, optionality or name mints nothing. DDL is never refused
for the layout's sake. Rows of every earlier generation stay readable through their own
descriptors. A table whose ring of 65535 numbers is full refuses a statement that would mint, and
every row stays readable.

**Proposed.** `ALTER TABLE … REWRITE` would re-encode every row below the newest generation at it,
so the table converges to one that was born on 4.0, and `MIGRATE EXISTING ROWS … USING` would apply
a transform to each row (`RFC_RECORD_LAYOUT_AND_MIGRATE.md` §5). The rewrite is opt-in per table
because it writes every row of a large table.

## 7. Tests

- **Unit and frozen fixtures.** The stored-value grammar, the envelope, descriptors and packed rows
  have unit tests and fuzz targets (`fuzz/fuzz_targets/`; packed rows and descriptors in
  `fuzz_packed_row.rs`). Frozen byte fixtures pin the envelope and documents
  (`surrealdb/core/src/kvs/compat/stored_record.rs`) and a set of packed rows with the
  descriptors they are read against (`surrealdb/catalog/src/packed/tests/fixtures.rs`), in both
  directions, so a change that moves a byte is red. Packed rows also have a seeded round-trip test
  over every physical kind, presence, open node and escape, which reads every row whole and field
  by field, and mutation tests that hold a field read to the whole decode
  (`surrealdb/catalog/src/packed/tests.rs`).
- **Upgrade pairs** (`language-tests/tests/upgrade/`): rows of a `SCHEMAFULL` and a schemaless table
  written by a released 3.x binary read beside rows the current build writes
  (`value/3.00.00_record_format_mixed.surql`), and layout generations minted over a 3.x table read
  every generation (`define/3.00.00_layout_generations.surql`). The suite runs forward only, so
  rollback is covered by unit tests of the gates.
- Each new release is added to the three version lists of `Makefile.ci.toml`, and a persisted
  format change extends the pair that produces the old shape.

## 8. Decisions taken

1. **The write gate:** a feature level in the store, a release per node, and a ledger entry for the
   format; single-writer engines raise themselves and others wait for an operator.
2. **Nonconforming rows:** kept as they are; the conformance record says so.
3. **DDL on a packed table:** every statement that changes the derived layout mints a generation;
   none is refused.
4. **Typed slots:** `bool`, `int`, `float`, `decimal`, `uuid`, `datetime`, `duration`, `string`,
   `bytes`, record keys, literal unions and points; everything else is a `Value` column.
5. **Downgrade:** a store raised to 4.0 does not return to 3.x other than through export and
   import; within 4.x it rolls back until something uses a feature the earlier release lacks.
