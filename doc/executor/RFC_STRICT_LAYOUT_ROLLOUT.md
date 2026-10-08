# RFC: Rolling out the strict row layout

**Status:** draft for discussion. Proposes how the `ROW` layout of
[`RFC_RECORD_LAYOUT_AND_MIGRATE.md`](RFC_RECORD_LAYOUT_AND_MIGRATE.md) reaches stored data: what it
represents, how it coexists with the `revision` encoding, when a cluster may write it, which tables
get it, and how DDL behaves on a table that has it. It names the decisions to make and the questions
for the storage and language owners.

---

## 1. Why this is worth doing soon

Every VM optimization that needs a field's kind to be known — typed instructions with no per-row
check, a builtin answered from a value's slot, a vectorized region over a typed column — needs the
field stored untagged at a known offset. The `strict_layout_against_revision` probe
(`core/src/exec/vm/tests.rs`) reads the same records in both encodings: in the `ROW` layout a field
read, a string comparison and a filter are each an order of magnitude cheaper, and a record is
about half the size. Until stored data uses the layout, none of that applies.

## 2. Settled

- **New `SCHEMAFULL` tables default to `ROW`** once the cluster may write it (§5), with
  `LAYOUT TAGGED` as the opt-out. Existing tables keep the `revision` encoding until their owner opts
  in (§6). Schemaless tables keep the tagged encoding.
- **The layout itself**: fields at offsets the generation assigns, a two-bit null map, strings as
  16-byte views whose length word says whether the string is ASCII, `id` in the key. The layout
  RFC §3 has the format; `core/src/exec/vm/layout.rs` implements it in memory, with round-trip tests.

## 3. What exists today

| mechanism | behaviour | where |
|---|---|---|
| major version `!v` | a u16; a node starts only on an exact match | `datastore/src/version/mod.rs`, `core/src/kvs/ds.rs` `check_version` |
| storage stamp `/!vs` | the semver of the newest build that started; only ever advances, and advances when the **first** upgraded node starts | `core/src/kvs/migration/mod.rs` `advance_stamp` |
| migration ledger `/!mg{id}` | a registered migration runs once, under a 60 s lease held by one node, on the startup that first finds it missing; other nodes wait up to 300 s; a crash re-runs the whole migration | `core/src/kvs/migration/mod.rs` |
| downgrade guard | a node refuses to start when the ledger holds a migration it does not know | `migration::check_downgrade` |
| cluster membership | a node record holds its id, heartbeat, gc and http endpoint — **not its version** | `catalog/src/node.rs` |
| mixed versions | documented as not supported; a node seeing a newer stamp warns and continues | `migration/mod.rs` |
| record encoding | a `revision`-encoded `Record { metadata, data }`; the first byte is the struct revision as a varint, `0x02` today, `0x01` for legacy rows | `catalog/src/record.rs`, `revision` 0.30 |
| `ALTER TABLE` | exists: schema mode, permissions, changefeed, comment, compact, type, inline caps | `syn/src/parser/stmt/alter.rs`, `core/src/ddl/alter/table.rs` |
| `DEFINE FIELD` on existing rows | applies to future writes only; existing rows are neither validated nor rewritten | `core/src/ddl/define/field.rs` |

Three consequences shape the proposal:

1. There is no way today to know that **every** node can read a new encoding: the stamp says only
   that one node could.
2. An existing `SCHEMAFULL` table may hold rows that its current field definitions would reject,
   because definitions never check what is already stored.
3. The migration ledger is built for short, idempotent catalog migrations, not for rewriting a
   table.

## 4. Representation

Every record a `SCHEMAFULL` table can hold is representable, and every one round-trips exactly:

| a field declared | is stored as |
|---|---|
| `int`, `float`, `bool` | 8, 8 and 1 bytes at its offset |
| `decimal`, `duration`, `uuid` | 16 bytes at its offset |
| `datetime` | 12 bytes at its offset (seconds, nanoseconds) |
| `string`, `bytes` | a 16-byte view |
| `record<t>` | a view of the key's encoded id |
| `array<T>`, `set<T>`, an object whose fields are declared | in the variable region, strict where every part is typed |
| `FLEXIBLE`, `any`, geometry, a union, a literal kind | in the tagged region, as `revision` encodes it today |
| `option<T>` | `T`'s slot, with the null map telling absent from `NONE` from `NULL` |

Record metadata (the edge flag and whatever `Metadata` gains) is a header flag field. A property test
encodes generated records of every declared shape and asserts that decoding them equals the
`revision` round trip of the same record.

## 5. Coexistence and when a cluster may write

**Reading both is unconditional.** A strict record starts with a marker byte the varint encoding
of a `revision` struct cannot produce (`0xFF`), and `Record`'s `kv_decode_value` dispatches on the
first byte. A test asserts that no revisioned `Record` can begin with it. A release that can read
the layout reads it everywhere, with no flag.

**Writing needs every node to read it.** An older node meeting a strict record fails to decode it,
so a cluster may write the layout only when no node older than the first reading release remains.
Nothing records that today (§3). Options:

| option | how the cluster knows | cost |
|---|---|---|
| **A. Operator switch** | an explicit statement (`ALTER SYSTEM ENABLE STORAGE FEATURE row_layout`, or a startup flag) records a feature key; nodes read it at startup and on a change | an operator step; a mistake is a cluster that cannot read its own data |
| **B. Node versions in membership** | each node's record carries its build version; writing is allowed while every live node is new enough | a membership format change; a node that joins old after the switch must be refused |
| **C. Two releases** | release N reads, release N+1 writes, relying on the supported upgrade path | the gain arrives a release later; still unsafe for a skipped or mixed upgrade |
| **D. Single-node first** | embedded, `mem`, `rocksdb`, `surrealkv` write at once (one process); distributed engines wait for A or B | two behaviours to explain |

Recommendation: **B, with D as its immediate case** — a single-process engine is trivially "every
node", and a distributed cluster writes once its membership shows every live node can read. B is
also what every later storage-format change needs, so it is worth building once. Once any strict
record exists, the downgrade guard refuses a build that cannot read it, through a ledger entry
recorded when the first strict write happens.

## 6. Which tables, and how existing ones move

- **New `SCHEMAFULL` tables** are `ROW` from their first write once §5 allows it. A new table starts
  empty and every write is coerced to its declared types, so every row conforms.
- **An existing table** opts in with `ALTER TABLE t LAYOUT ROW`. Its rows stay readable as they are
  (generation 0); each write rewrites the record it touches in the new layout; and a backfill
  rewrites the rest.
- **The backfill is a resumable program, not a ledgered migration**: a range at a time, each record
  with a conditional put so it never overwrites a concurrent writer, its progress the last key it
  committed, any node able to resume it
  ([`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md)). Until it finishes, readers
  handle both generations per row.
- **Rows that do not conform.** Because `DEFINE FIELD` never checked existing rows, an existing table
  can hold a value its schema rejects. Options for `ALTER TABLE … LAYOUT ROW`:
  1. **Refuse** when a scan finds one, naming the record; the owner fixes or migrates it first.
  2. **Keep it tagged**: a nonconforming record stays in generation 0 and is read as today; only
     conforming records move.
  3. **Coerce on backfill**, as a write would, and fail the record that cannot be.

  Recommendation: **1**, with the scan run before any record is rewritten, so the answer is known up
  front and nothing is half-converted. 2 leaves the table permanently mixed, and 3 changes stored data.

## 7. DDL on a strict table

A strict table's rows all have the fields of their generation at its offsets, so a change to the
fields is a new generation (layout RFC §4). Options:

| option | adding a field, widening to `option<T>`, adding a default | narrowing or changing a kind |
|---|---|---|
| **A. Additive changes are automatic** | a new generation with a transform that cannot fail: absent fields read `NONE` or the default | refused outside `MIGRATE`, which supplies the transform |
| **B. Everything through `MIGRATE`** | refused outside `MIGRATE` | refused outside `MIGRATE` |
| **C. Everything automatic** | automatic | automatic, failing per row when a value does not convert |

Recommendation: **A**. Adding fields works exactly as it does today; only changes that can fail ask
for more, and those changes today leave existing rows inconsistent with their schema — requiring a
transform is closer to a fix than a restriction. C breaks the layout RFC's rule that a migration
never fails.

## 8. Release plan and tests

1. **Reading.** The strict decoder, the marker dispatch, and the representation property test. No
   behaviour change; ships whenever it is ready.
2. **The write gate** of §5: node versions in membership, the ledger entry and downgrade refusal.
3. **Writing for new tables**, behind the gate: `DEFINE TABLE … LAYOUT ROW | TAGGED`, the default for
   new `SCHEMAFULL` tables, generations in the catalog, additive DDL (§7).
4. **Existing tables**: `ALTER TABLE … LAYOUT ROW`, the conformance scan, the backfill program.
5. **`MIGRATE`** for changes that can fail.

Each step brings the upgrade-test obligations of `CLAUDE.md`: pairs in `language-tests/tests/upgrade/`
where an older release writes and the newer reads — generation-0 rows read by a release that writes
the layout; strict rows written by one release and read by the next; a table mid-backfill read
across an upgrade — and the new releases added to all three version lists in `Makefile.ci.toml`.
The catalog changes (generations, field ids, the feature and ledger keys) are revisioned structs, so
`revision.lock` and the frozen byte fixtures move with them.

## 9. Decisions to make

1. **The write gate** (§5): operator switch, node versions in membership, two releases, or
   single-node first.
2. **Nonconforming rows** in an existing table (§6): refuse, keep tagged, or coerce.
3. **DDL on a strict table** (§7): additive automatic, everything through `MIGRATE`, or everything
   automatic.
4. **Which kinds get typed slots in the first version** (§4), and whether a strict table may hold a
   tagged region at all.
5. **Downgrade**: whether a cluster that has written strict records may ever run an older build
   again other than through export and import.

## 10. Questions for the storage and language owners

- Is a node version in the membership record acceptable, and is there already a plan for mixed-version
  clusters that this should fit into?
- Who owns the record-encoding change, and which release carries step 1?
- Does a default change for new `SCHEMAFULL` tables need a user-facing opt-out beyond
  `LAYOUT TAGGED`, such as a database-level default?
- Is refusing `ALTER TABLE … LAYOUT ROW` on nonconforming rows acceptable, or do existing users need
  a path that converts what it can?
- Should `DEFINE FIELD` begin validating existing rows on strict tables, since a strict table can
  answer that question cheaply?
