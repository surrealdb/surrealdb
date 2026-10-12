# RFC: Materializations

**Status:** draft. Proposes one protocol for every structure the engine keeps as
a function of a table's records — an index, a materialized view, a record
rewritten into a new layout or a new field type — built while writers
continue, resumable on any node, and maintained by the writes themselves once
it is complete. A materialization is a triggered program of the durable
runtime; what this RFC adds is how the program and concurrent writers keep the
structure correct between them.

Companions: [`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md),
whose triggered programs, checkpoints and takeover a materialization runs on;
[`RFC_RECORD_LAYOUT_AND_MIGRATE.md`](RFC_RECORD_LAYOUT_AND_MIGRATE.md), whose
proposed migration backfill is one kind of materialization here;
[`RFC_RECORD_FORMAT_ROLLOUT.md`](RFC_RECORD_FORMAT_ROLLOUT.md).

---

## 1. Why

Five things today keep state that is a function of a table's records, and each
does it its own way:

| structure | how it is built on existing records | how it resumes |
|---|---|---|
| an index (`DEFINE INDEX`, `REBUILD INDEX`) | the index builder (`core/src/kvs/index/`): a durable build protocol with build-state, admission-ticket, queued-mutation, scan-pointer and reservation key families, owner heartbeats and generations | its own task lease (`IndexBuildResume`) |
| a materialized view (`DEFINE TABLE … AS SELECT`) | one `SELECT` over the source tables inside the defining transaction (`ddl/define/table.rs`) | it does not; the transaction is unbounded |
| an aggregated view | one grouped `SELECT` inside the defining transaction | it does not |
| a field's new type (`DEFINE FIELD … OVERWRITE TYPE`) | not at all: existing records keep the old type | — |
| a layout generation (`MIGRATE`, proposed) | a conditional-put backfill | a triggered program |

They share one problem: keep a derived structure equal to a function of the
records while writers keep writing, without a transaction the size of the
table, and without losing the work when a node stops. The index builder solves
it correctly and at length; the view initialisation does not solve it; a type
change does not attempt it. One protocol solves it once.

## 2. A materialization

A materialization *M* of table *t* is:

- **a transform** *T*, a bytecode program compiled from *M*'s definition, from a record's
  before-image and after-image to the mutations of *M*'s target (§3);
- **a target**: a key range *M* owns (index entries, a view's rows), or *t*'s records themselves
  (§5);
- **a read policy**: whether a reader may use the target before *M* is complete (§6);
- **a state**, in the KV store: `Building`, `Online`, `Error` or `Retired`, the definition it was
  compiled from by content hash, and, while `Building`, the backfill's checkpoint.

**The invariant.** When *M* is `Online`, for every record *r* of *t* the target holds exactly
what *T*(∅, *r*) produces, and nothing else. While *M* is `Building`, the same holds for every
record the protocol has *marked* (§4), and the target holds nothing for any other.

Two things keep it:

- **The writers.** Every write to *t* calls *T*(before, after), in the writing transaction, for
  every materialization of *t* that is `Building` or `Online`. This is the called work of the
  durable runtime (its §2): a write's index entries, a view's row, are visible to the statement
  after it, and a unique violation fails the write that causes it.
- **The backfill.** A triggered program scans *t* and applies *T*(∅, *r*) to every record not yet
  marked, in batches, each batch with its checkpoint in one transaction.

## 3. The transform

*T* is compiled from the definition when the definition is committed, stored
by content hash, and recompiled from the definition wherever it is needed; its
bytecode is never the durable state (§8). Its program header (runtime RFC §3)
is checked against what its kind requires:

| kind | *T*(before, after) produces | *T* must be |
|---|---|---|
| B-tree / unique / `COUNT` index | the entries to delete for `before`, to put for `after` | reads the row only; may raise only a unique violation |
| full-text / HNSW / DiskANN index | the structure's mutations for the record's change | reads the row and the structure's own keys; see §10, question 4 |
| materialized view | the view row to delete, to put | reads the row only |
| layout generation, field type change | the record rewritten into the new generation | the migration predicate: pure, reads `$row` only, infallible (layout RFC §5.1) |

The backfill applies *T* with no before-image. The writers apply it with both.
The one transform serves both, so what a write maintains and what the backfill
builds cannot disagree.

*T* may call **engine intrinsics**: native operations linked into the
engine, such as inserting a vector into an HNSW or DiskANN graph, or tokenising
and posting a full-text document. An intrinsic is an instruction with declared
semantics, implemented in Rust. What the protocol needs from it is not that it
is bytecode, but that every effect it has goes through the transaction, that
it is deterministic given the transaction's state (so a retried batch computes
the same effects), and that it declares its effects, footprint and
fallibility as a builtin declares its `EffectSet`. Sandboxed forms — WASM, a
dynamically loaded library — are for code a user supplies (Surrealism
modules), not for the engine's own structures, which should not pay their cost.

### 3.1 Index kinds

An index kind is a Rust implementation of one trait, dispatched statically
(an enum over the kinds the engine links), with a declaration shaped as a
Surrealism module's exports manifest is:

- **`maintain(handle, index, batch)`** — applies the mutations for a batch of
  `(before, after)` pairs. The backfill calls it with no before-images; a
  write calls it with a batch of one or of its statement's rows. A kind
  parallelises within a batch where only it knows how: an HNSW insert searches
  for candidates against the snapshot in parallel and applies its graph edits
  in order.
- **`prepare(index, after)`**, optional — the pure part of maintenance
  (tokenising, quantising, normalising), which the planner may run anywhere
  and fan out freely; `maintain` then receives its output.
- **`scan(handle, index, …)`** — the query side, as the index access
  operators call it.
- **the declaration** — for each operation its `EffectSet`, whether it can
  fail and how, whether it is async, and its footprint: per record (a
  record's mutations touch keys no other record's do, save equal unique
  keys), or shared state it names (a graph's neighbourhoods, BM25 totals, a
  `COUNT` total).

`handle` is lent for one call and revoked on return, as a Surrealism
invocation's transaction handle is. It offers key-value reads, writes and
range scans, and nothing else, confined to the index's own key band: an
access outside the band is refused where it is made, as
`NoWriteFrame::SurrealismReadOnly` refuses a write, so the footprint the
planner reasons from is enforced, not trusted. The planner fans maintenance
out across batches whose footprints are disjoint and serialises it across a
shared one.

The calls are internal builtins: the function registry holds them, the
logical plan and the VM's call instruction reach them, and the parser never
resolves their names, so no statement a user writes can reach an index's keys
around its permissions or out of step with its records.

A kind's output is a persisted format. The same records must produce the same
entries on every release that can resume a backfill another release began, so
a change to a tokeniser, a quantiser or a graph's level assignment is a format
change with upgrade pairs. Level assignment and any other randomness are
seeded from the record, never from a thread's generator, and any in-memory
state a kind keeps is a cache that answers only a snapshot it was built from.

A Surrealism module can implement the same trait through its runtime, with
the handle crossing the WASM boundary, so a user-supplied index kind is the
same interface at a sandbox's cost.

## 4. Marking: writers and the backfill together

### 4.1 The race a mark closes

A backfill batch reads record *r* in its snapshot and writes *r*'s target
mutations. A writer changing *r* concurrently writes its own mutations for the
new *r*. Under snapshot isolation the two transactions conflict only if they
write a common key, and neither writes a key the other does: both commit, and
the target holds the backfill's mutations for the old *r*.

### 4.2 The mark

Each record has a **mark** while *M* is `Building`, and both parties write it:

- **A writer**, calling *T* for *r*, reads *r*'s mark first. With no mark, the backfill has not
  reached *r*, so the target holds nothing for *r*: the writer applies *T*(∅, after) and writes the
  mark. With a mark, it applies *T*(before, after) as an `Online` materialization would. A delete
  with no mark writes only the mark.
- **The backfill**, for each record of a batch, skips it when marked; otherwise it applies *T*(∅,
  *r*) and writes the mark.

Both write the mark, so in the race of §4.1 one transaction commits and the
other aborts on the write-write conflict. A backfill batch that aborts retries
from its checkpoint, finds the mark, and skips the record. A writer that aborts
retries as any conflicted write does. No other key is shared, so writers never
contend with the backfill or with each other except on the one record they
both touch.

### 4.3 Where the mark lives

- **A derived target** marks with a key in *M*'s own band, one per record, keyed by the record's
  key. The band is dropped with one range delete when *M* goes `Online` (§7).
- **An in-place target** marks with the record itself: its layout generation. A record in the new
  generation is marked; the backfill's write is a conditional put, applied only while the record is
  still in the old generation (layout RFC §5.3). The record key is already the key both parties
  write, so no extra key exists.

### 4.4 Writers that do not know *M* yet

Marks protect only a writer that sees *M*. A writer learns which
materializations its table has from the catalog, so a transaction that began
before *M*'s definition committed does not see it: it writes neither the mark
nor the target's mutations. If it commits after a backfill batch read the
record it changes, the batch's mutations for the old record commit unopposed.

The backfill therefore starts only once no transaction that began before the
definition committed can still commit a write to *t*. That is a barrier across
every node, not one commit: each node publishes the start of its oldest open
write transaction (or holds a schema lease it renews), and the backfill waits
until every node's is later than the definition's commit. It is the
write-only-before-backfill step of an online schema change (F1): *M* is
maintained by every write before anything reads the records it has not
reached.

A single-node datastore meets the barrier by waiting for its own open write
transactions to finish.

### 4.5 What it costs, and what it removes

While *M* is `Building`, a write to *t* costs one point read (the mark) per
materialization, and each record costs one small key, written by whichever
party reaches it first. Nothing costs anything once *M* is `Online`.

It replaces, for every kind: writer admission, queued mutations and their
replay, per-record scan pointers, reservations, and generation fencing of
queued work. Batches are idempotent — a replayed batch skips what it marked —
so a checkpoint may lag the work it describes without harm.

## 5. Targets

| | derived | in place |
|---|---|---|
| holds | entries in *M*'s band | *t*'s records |
| mark | a key per record in *M*'s band | the record's generation |
| `Online` when | the scan completes | no record of the old generation remains |
| one at a time per table | no | yes: one generation in migration (layout RFC §4) |

A derived *T* reads the **logical** record: a record still in an old
generation is read through the migration's transform first. An index built
during a migration therefore describes the new generation, and a migration may
change a field an index reads — the index is rebuilt as a second
materialization, `Online` before the new generation is visible (lifting the
layout RFC's §5.4 restriction).

## 6. Read policies

- **Gated.** A reader uses the target only when *M* is `Online`. The query planner considers an
  index only then, as it considers only a durable-`Online` index today. Indexes and materialized
  views are gated.
- **Read-through.** A reader meeting a record *T* has not reached applies *T* to it and reads the
  result, without writing it. This is what makes an in-place materialization invisible: a read
  before, during and after the backfill answers the same. It requires *T* to be pure and
  infallible, which the migration predicate proves. Layout generations and field type changes are
  read-through.

## 7. Lifecycle

- **Defining.** The definition, *M*'s state as `Building`, and the backfill's invocation are
  written in the defining statement's transaction. *M* exists exactly when the definition commits,
  and writers maintain it from the next transaction on.
- **Backfilling.** The backfill is a triggered program (runtime RFC §4): it checkpoints its scan
  cursor with each batch, and any node resumes it from the checkpoint when its owner stalls.
- **Completing.** One transaction sets *M* `Online` and enqueues *M*'s mark band for reclaim. For
  an in-place target, completion is the scan finding no record of the old generation, and retires
  the old generation's reader.
- **Failing.** A backfill batch that raises — a unique violation between existing records, a
  structure write that fails — sets *M* `Error` with the reason. Writers stop calling *T* for an
  `Error` materialization, so a failed build never blocks a write. `INFO` reports the reason.
- **Rebuilding, redefining, removing.** Each gives the definition a fresh materialization id. The
  old *M* is `Retired`: its target and marks are enqueued for background reclaim, as a replaced
  index's data is today, and the new *M* starts from an empty band.

The lifecycle above is `DEFINE INDEX ... CONCURRENTLY`: the defining statement
returns once *M* is defined. A `DEFINE INDEX` without `CONCURRENTLY` keeps its
blocking contract: the statement answers only once the index is built, and a
failed build fails the statement, leaving no definition behind. It runs the
same backfill and waits for it, and on a failure removes the definition, its
state record and its band before it answers with the error. While it waits,
*M* is `Building` to every other transaction, as a concurrent one is, so the
writes in the interim are maintained and marked like any others (§4) and the
barrier of §4.4 applies to it unchanged. What a blocking definition cannot
offer is that no other transaction observes it before it completes: `INFO`
run during the build reports it `Building` (§10, question 1).

## 8. What is durable

- *M*'s state record: its id, kind, table, the definition's content hash, its state, the reason for
  an `Error`, and the backfill's checkpoint — the key of the last record its last committed batch
  covered.
- *M*'s band: marks, and a derived target's entries.
- The backfill's invocation, owner and epoch, as the runtime RFC defines them.

The transform's bytecode is not durable. A checkpoint is a cursor and nothing
else: the backfill yields only between batches, where it holds no registers,
so a materialization resumes under any release that can compile its
definition, whatever the instruction set has become. Whether other triggered
programs checkpoint registers is the runtime RFC's decision, and this RFC does
not depend on it.

Every format above is versioned and has upgrade pairs in
`language-tests/tests/upgrade/`, as `CLAUDE.md` requires: a materialization
left `Building` by release N, resumed and completed by release N+1; a
materialization `Online` under N, maintained by writes under N+1.

## 9. Retiring the index builder

A release that carries this RFC must read what the current builder leaves:

- **A build in progress** under the builder's protocol when the node upgrades is restarted as a
  materialization with a fresh id: its build state, queue and pointers are enqueued for reclaim,
  and the new backfill scans from the start. This is `REBUILD INDEX`'s behaviour today, so it
  loses no guarantee.
- **An `Online` index** needs nothing: its entries are the target of a materialization that is
  already `Online`, and the release writes its state record the first time it reads the index.

The builder's key families, admission and replay, and the `IndexBuildResume`
task lease are removed in the release after, once no node that writes them can
remain in a cluster. The view initialisation's synchronous `SELECT` is replaced
in the same release as views become materializations.

## 10. Open questions

1. **Blocking definitions.** A blocking `DEFINE INDEX` fails as a whole (§7) by removing its
   definition after a failed backfill, so the definition is visible, as `Building`, while it builds.
   The alternative that hides it — the whole backfill in the defining transaction — needs one
   transaction to read the table and write the index, past the size and duration limits of
   distributed backends, and under snapshot isolation still misses a writer that changes a record
   after the transaction read it (it writes no key the definition writes). Is the visible `Building`
   state acceptable for a blocking definition?
2. **Field type changes on existing records.** A type change never touches existing records today.
   As a read-through materialization it converts them. That is a language change for schemaless
   and non-strict tables: is it wanted there, or only for strict tables under `MIGRATE`? And a
   conversion that can fail for some records must be refused before *M* is defined (a pre-scan, as
   the rollout RFC proposes for strict tables) — is that the contract?
3. **Unique violations during a build.** A violation between two existing records fails *M* (§7).
   A violation between a written record and an existing one the backfill has not reached is found
   by the backfill, not the write. Should the write fail instead, which needs the writer to probe
   unmarked records' entries — the queue's job today?
4. **Full-text and vector structures.** Their transforms are intrinsics (§3), but their mutations
   for one record rewrite shared state — posting lists, a graph's neighbourhoods — that another
   record's mutations also touch, so two records' maintenance conflicts even when both are marked.
   Serialise one index's maintenance (a per-index queue or lock key), or let an approximate index
   be a materialization permitted to lag its writes, which vector search may tolerate where exact
   indexes cannot?
5. **Aggregated views.** A group's row is a function of many records, so *T* for one record is a
   delta to shared state, as for question 4. Same question, and whether it waits for a second
   version.
6. **Mark cost at scale.** A build over *n* records writes *n* marks, and their range delete. An
   alternative marks only records written ahead of the backfill's cursor, which needs writers to
   read the cursor in a way that conflicts with the backfill's advance of it; is that worth the
   contention it adds to one key?
7. **The key bands** for state records and marks, and how a completed materialization's records
   are compacted.
8. **The barrier's mechanism** (§4.4): per-node oldest-transaction watermarks through the node
   registry, or schema leases. The index builder's blocking build has the same exposure — its
   definition is invisible to other transactions while it builds, so their writes reach neither
   the index nor the build's queue — which a reproduction should confirm or rule out; if it is
   real, it is a defect in the current release, independent of this RFC.

## 11. Order of work

1. The index-kind trait of §3.1, with the existing B-tree, unique, `COUNT`, full-text, HNSW and
   DiskANN code behind it, each checked for determinism and snapshot-confined caches; the VM's
   write path and today's document path both calling it. It is needed whatever else is decided.
2. Per-record index maintenance as a bytecode subroutine calling it, for B-tree, unique and
   `COUNT` indexes, checked against the language suite.
3. The materialization state record and marks, with the backfill as a triggered program, for those
   index kinds; `DEFINE INDEX` and `REBUILD INDEX` on it; the upgrade pairs of §8 and §9.
4. Materialized views on it, replacing the synchronous initialisation.
5. Layout generations and field type changes as in-place materializations, with `MIGRATE`.
6. Full-text, vector and aggregated structures, as §10 questions 4 and 5 decide.
