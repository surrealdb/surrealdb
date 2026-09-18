# Upgrading SurrealDB

This document records per-release operational notes for SurrealDB
administrators: wire-format rotations, removed features, configuration
changes, and other impacts that aren't apparent from version numbers
alone. Read this before deploying a new release onto an existing
database.

For the release **process** (how releases are cut and shipped), see
[`RELEASING.md`](./RELEASING.md). For end-user release notes, see
<https://surrealdb.com/releases>.

## v3.2.5 — ANN pending entries at a new revision

**Impact**:

- An HNSW or DiskANN index that was written by an earlier release while any
  indexed record's id held a number inside an array or object — `chunk:[42,
  'intro']`, `doc:{tenant: 1}` — keeps a stale doc-ID mapping for those records.
  They are missing from kNN results, with no error; the query simply returns
  fewer rows than the `k` it asked for. Run `REBUILD INDEX <name> ON <table>`
  once per affected index.
- In a mixed-version cluster, a node on an older release fails kNN queries
  against an HNSW or DiskANN index once an upgraded node has written to it, and
  also fails a write to any record that index already has an update queued for,
  and its own background compaction of that queue. Upgrade every node, or stop
  both writing to and querying those indexes from the older ones until you
  have — routing only reads away is not enough.

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
drains the queue. The window opens on the first write an upgraded node makes to
the index and closes when every node is upgraded; it does not depend on the ids
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
record is stored under. This release stops such a mapping being created, but it
does not reach back: an index built before it keeps the mappings it already has.

`REBUILD INDEX` replaces them. The mapping is scoped to the index that owns it,
so a rebuild clears that index's forward and reverse doc-ID entries and derives
them again from the records themselves, under the spelling each record is
actually stored under. There is no automatic repair — an index left un-rebuilt
keeps returning short.

One case a rebuild does not fix, because it predates this release and is not
about the queue: two records whose ids differ only in the numeric variant of a
nested value — `c:[1]` and `c:[1dec]`, `c:{k: 1}` and `c:{k: 1f}` — still share
one forward doc-ID entry, because that key uses the same kind-erasing codec. One
of the two stays absent from that index's results after a rebuild as before it.
Mixing such ids in one indexed table is the only way to reach it.

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
