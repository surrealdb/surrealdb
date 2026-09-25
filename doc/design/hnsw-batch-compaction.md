# HNSW batch compaction

How queued HNSW updates become graph, how one batch is built on many cores at
once, and why doing so never leaves the resident graph holding a topology that
did not commit.

The contracts live in the module documentation of
`surrealdb/idx/src/trees/hnsw/batch.rs`, `hnsw/index.rs` and
`surrealdb/idx/src/parallel.rs`; this document is the map between them and the
argument that ties them together.

## Where materialisation happens

An HNSW write never touches the graph. `HnswIndex::index` folds the record's
old and new vectors into one record-keyed pending entry (`!hr`), and a kNN
search scores every pending entry by hand alongside the graph it searches.
Compaction is what turns pendings into graph, one bounded batch at a time:

1. **Plan** (read transaction) — `HnswIndex::prepare_compaction` captures at
   most `HNSW_COMPACTION_MAX_PENDING_KEYS` pending entries, as their exact
   bytes, coalesced by record identity.
2. **Apply** (write transaction) — `HnswIndex::apply_compaction` advances the
   `!hg` compaction generation from the value the plan observed, deletes each
   captured entry on the condition that it still holds the captured bytes, and
   folds the plan's operations into the graph.
3. **Commit** — by the caller: the build's own drain
   (`Building::compact_hnsw_pendings`, from `DEFINE INDEX` / `REBUILD INDEX`)
   or background maintenance (`Datastore::process_hnsw_compaction`).

Each batch is one transaction and, per index, one batch is in flight. A batch's
graph changes are derived from the graph every earlier batch committed, and
every insertion rewrites the adjacency lists of the neighbours it links to, so
two batches built concurrently would conflict on nearly every commit — the
`!hg` generation lets exactly one of them win. That rules out parallelism
*across* batches. Everything below is parallelism *inside* one batch's apply.

A `CONCURRENTLY` build over an existing table reaches `Online` once every row
has a pending entry, and only then starts the drain, so the drain is where the
whole graph of that table gets built. Graph construction is dominated by
distance evaluation — the neighbourhood search each insertion runs at `EFC`,
and the re-pruning of every neighbour whose degree an insertion takes past its
layer's cap — and applying one operation at a time runs all of it on one core.

## Two ways to apply a batch

`apply_compaction` sorts a plan into one of two shapes before it writes
anything, so a plan never switches shape part-way.

* **Staged** — no operation removes a vector from the graph: every operation
  adds vectors, attaches documents to vectors the graph already holds, or only
  drops a document mapping. This is the shape of every batch a build drain
  applies, and of every batch under an insert-only workload. It is built on
  many tasks and published after the commit, as described below.
* **In place** — at least one operation removes a vector, or the index engines
  are configured `serial`. The operations apply one at a time under the graph
  write lock, changing the resident graph as they write. Removal repair
  re-searches the neighbourhood a removal has just changed, which is why a
  removal-carrying batch stays on this path.

`serial` keeps the one-at-a-time algorithm selectable for every batch: it is
the lever to fall back on, and the oracle the staged path's recall is measured
against.

## The staged batch

Let *G₀* be the graph as committed before this batch. The apply works in four
steps, and only the transaction sees what they produce.

**0. Allocate** (write lock, serial). The resident graph is brought level with
the write transaction's view of the store, under the write lock, as an
in-place apply does. Then, per operation, in plan order: the record's document
id is resolved (allocating one if needed), and each vector is matched against
the vector-to-documents mapping (`!hh` / `!hv`) through the write transaction.
A vector the index already holds — committed, or earlier in this batch —
gains the document and changes no graph topology. Every other vector becomes
a *new element*: the next element id, a level drawn from the index's level
generator, and its `!he` and mapping entries written into the transaction. The
mapping logic is the in-place path's own (`VecDocs::insert`); the two paths
differ only in where a new element goes (`ElementSink`). The write lock is
then downgraded to a read lock, with no writer let in between, so *G₀* cannot
change while the batch reads it.

**1. Select** (read lock, parallel over new elements). For each new element
*i* at level *lᵢ*, the serial insertion's search runs against *G₀* only:
greedy descent from *G₀*'s entry point through the layers above *lᵢ*, then an
`EFC` search at each layer from *lᵢ* down to 0, seeded from the layer above.
*G₀* cannot see the batch's own elements, so each layer's candidates also take
the exact nearest batch elements at that level. The two lists are merged, cut
to the `EFC` nearest, and passed to the index's neighbour selection heuristic,
giving *i*'s out-edges per layer.

**2. Link** (read lock, parallel over touched nodes). Every out-edge *i → n*
owes *n* a back-edge. The back-edges are grouped by target, and each target is
resolved once: its base list (its *G₀* edges, or for a new element its own
out-edges) joined with every batch element that selected it, re-pruned by the
heuristic if that takes it past its layer's cap.

**3. Stage** (serial). Every resolved node's `!hn` entry and the new `!hs`
state — each changed layer's version advanced by one, the layers the batch
adds, the entry point, the next element id — are written into the
transaction. A commit action (`PublishBatch`) and a rollback action
(`RollBackBatch`) are registered on it, and the read lock is released.

Within the batch nothing conflicts with anything else. Element ids are
allocated serially in step 0, before any parallel work; each task writes only
its own results; and because step 2 groups back-edges by target, every
`(layer, node)` edge list is resolved and written by exactly one job. Against
concurrent writers the batch conflicts only where the compaction protocol
already does: a captured pending that a writer rewrites fails its conditional
delete.

No two jobs ever work on the same record, however often it changed before
compaction. A record's writes fold into one record-keyed pending entry, which
keeps the first write's graph baseline and the last write's vectors, so the
record-keyed range gives a plan one operation per record. Entries of the legacy
append-keyed layout coalesce by the identity they carry — a doc-ID, or a record
key before the record has one — and are applied before the record-keyed range,
in plan order. Step 0 resolves every operation's record serially, and the jobs
of steps 1 and 2 work on graph elements and nodes, not records. A write that
rewrites a captured pending after the plan was read makes that pending's
conditional delete fail, so the batch is abandoned and the next plan takes the
record as rewritten.

The commit then either lands or does not.

* **It lands**: the commit action takes the write lock and publishes. If the
  resident graph still stands exactly at *G₀*, the resolved nodes, the new
  layers and the new state are installed. If it stands anywhere else, it has
  been reloaded since — from this commit or from another committed state — and
  is left for the next state check to reconcile with the store. Either way the
  new elements' vectors and the changed document sets are then entered into
  the process-wide caches: they are committed.
* **It does not land**: the commit action is discarded with the transaction,
  and the rollback action runs. Normally it finds the resident graph at *G₀*
  and has nothing to do; see the next section for when it does.

## Why a staged batch cannot publish a topology that never committed

The resident graph and the index's element caches change in only three ways:
a state check that reloads layers from a transaction's view of the store, an
in-place apply, and a staged batch's publication. A staged batch computes
against *G₀* under a read lock and writes only into its own transaction and its
own memory, and its commit action runs only once the commit has succeeded. So:

* **Rollback leaves nothing behind.** A conflict, a failed commit, a cancelled
  transaction or an apply error leaves the resident graph at *G₀*, which is
  committed, and the caches without the batch's elements. The caller discards
  nothing, and the next batch starts from the graph as it stands.
* **Lease expiry cannot make it happen.** Two appliers that start from the same
  base — a lease that expired mid-batch, or a build drain and background
  maintenance racing — both stage, and at most one commits, because both
  advance the same `!hg` generation. Only the one that commits publishes.
* **A handle held across a failed batch reads committed topology.** A reader
  that cloned a `SharedHnswIndex` before a staged batch failed reads *G₀*
  throughout, because nothing replaced it.
* **Versions stay unique to committed states.** Every layer version a staged
  batch puts into the resident graph was written by the commit that publishes
  it, so a version still identifies one committed topology, and the state
  check stays sound.
* **Publication is safe to lose a race.** Between the commit and the
  publication, a reader whose snapshot includes the commit may reload the
  changed layers itself, and one whose snapshot predates *G₀* may reload an
  older state. Publication installs its nodes only onto the exact base they
  were derived from; onto anything else it installs nothing.
* **A search reads the graph of its own snapshot.** A search brings the
  resident graph level with its transaction's view of the store and walks it
  under one read guard, taken before the check and held until the walk ends;
  a check that has to reload downgrades its write guard rather than releasing
  it. Nothing that changes the resident graph — a publication, an in-place
  apply, another reader's reload — can run between the check and the walk. A
  search that walked a graph from a later snapshot would meet nodes for records
  its own snapshot still holds as pendings, under doc-IDs it cannot resolve,
  and could be pointed at an entry point whose vector it cannot read; one that
  walked a graph from an earlier snapshot would miss records whose pendings
  its snapshot no longer holds.

One case needs the rollback action. A transaction may carry a second apply for
the same index, or a search: either reads the graph back through that
transaction, and so can reload into the resident graph the uncommitted state
the first batch staged there. If that transaction then does not commit, the
resident graph no longer stands at *G₀*, and the rollback action invalidates
it. The compaction callers commit each batch in its own transaction, so they
never reach this case; the tests that apply several batches in one transaction
do.

## Invalidation

A batch that reached the resident graph and did not commit leaves it holding
state the store never took, and layer versions cannot reveal that: a version is
a counter each writer advances in its own copy, so a copy that rolled back and
a peer that committed the same batch from the same base stand at the same
versions with different topologies. Each index therefore counts invalidations.
A rollback action that finds the resident graph compromised advances the
count, and a resident graph that has not reloaded in full since the count last
moved reloads every layer at its next state check.

The rollback of a staged batch advances the count only when the graph no
longer stands at *G₀*. The rollback of an in-place batch always does — an
in-place batch has changed the graph by the time it can roll back — and also
drops the index's cache entries, since it entered its elements' vectors as it
applied. The count lives on the index, not the registry entry, so every holder
of the index reloads: the registry's, and every handle cloned out of it. The
compaction callers still discard the registry entry after a failed in-place
batch, so the next access builds a new index outright.

## Recall

A batch built this way is a different graph from one built an operation at a
time. Three choices keep it comparable:

* **Batch elements see each other exactly.** An element inserted one at a time
  finds earlier batch elements only if its search reaches them; here each
  element is offered the exact nearest batch elements at every layer. This is
  what keeps the first batch of a drain over an empty graph — where *G₀* has
  nothing to search — from producing isolated nodes.
* **Every layer keeps its cap.** Out-edges come from the same heuristic, and
  every node whose degree the batch takes past its cap is re-pruned by it.
* **Pruning sees the whole batch.** A neighbour chosen by several elements of
  one batch is pruned once over all of them, so its final list is chosen from
  every offered edge rather than depending on arrival order.

With the neighbour-extension variants of the heuristic, only *G₀* nodes are
extended through their *G₀* edges: a batch element has no edges until the
batch publishes, and every batch element is already a direct candidate.

The graph a batch produces is a function of *G₀* and the plan alone. Level
draws happen serially in step 0, every chosen neighbour list is put in
element-id order, and back-edges are grouped by target in element-id order, so
the same plan over the same *G₀* produces the same graph whether it runs on one
task or on sixty-four. The tests pin that equality directly rather than timing
the build.

## Parallelism

Steps 1 and 2 run through `parallel::run_jobs`: tokio tasks spawned onto the
current runtime, each holding a shared owned read guard on the graph, the
batch's own elements, and the write transaction. Jobs are handed out one at a
time, results come back in job order, a failing job fails the batch, a
panicking one resumes its panic on the caller, and dropping the apply aborts
the tasks. One process-wide gate bounds the jobs running at once to the
machine's available parallelism, because background maintenance compacts
every index with outstanding work at the same time.

The number of tasks per batch is `hnsw_build_parallelism`
(`SURREAL_HNSW_BUILD_PARALLELISM`): `auto` (the default) uses the machine's
available parallelism, a number fixes the count, and `serial` selects the
in-place path for every batch. Parallelism is a property of the machine, not
of the index, so it is configuration rather than part of `DEFINE INDEX`.
Without a tokio runtime to spawn onto (wasm, or an embedding that runs none),
or with one task, the steps run inline on the applying task; the graph is the
same.

A vector the tasks read is either an element of *G₀* — committed before the
batch began, and immutable once committed — or one of the batch's own elements,
served straight from the batch and never read back from the store. So a task
never needs the batch's write transaction to read: a vector that misses the
process-wide cache is read through a read-only snapshot the job leases from a
pool opened alongside the batch (`parallel::SnapshotPool`), one per running
job, reused across jobs and cancelled when the batch has been built. That
matters because an engine that runs one transaction's operations one at a
time — RocksDB — would otherwise queue every task's misses behind the write
transaction, and an index larger than the vector cache misses on most reads.
A snapshot that opens after another compactor committed can only belong to a
batch that will fail on the `!hg` generation, so what it reads never
publishes.

The same misses fill the process-wide vector cache, which tracks per index
which entries it holds so an index can be dropped from it at once. That
tracker is split over independently locked shards by id, and the paths every
cache fill takes hold its map only shared, so tasks filling the cache for one
index contend only when their ids fall in the same shard.

## Not covered here

* **Parallel removal repair.** A removal-carrying batch stays in place. Repair
  re-searches the neighbourhood a removal just changed, which a staged batch
  would have to answer from an overlay of its own changes.
* **A parallel initial scan.** The index engines accept concurrent writers of
  non-conflicting data, so the scan that enumerates a table into pendings could
  be split by record range across transactions. That changes the build's
  checkpoint and ownership protocol, which is its own design.
* **Building the initial graph straight from the table scan**, instead of
  enumerating every row into a pending entry first.
* **What `ready` promises.** `Online` still means every row has been
  enumerated; `INFO FOR INDEX` reports `compacting` until the drain has folded
  them in.
* **A progress count for the drain.** `compacting` stays an existence test.
