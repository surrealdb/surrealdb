# Index build and materialisation

How an index gets from `DEFINE INDEX` to answering at index speed. Two
concerns meet on that path: a durable protocol that makes an index queryable,
holding every write it admitted, and a materialiser that folds queued writes
into the structure queries read. This document states what each promises, why
there is one of each, and what a blocking statement waits for. The protocol's
own contracts are in the module documentation under
`surrealdb/core/src/kvs/index/`, and how one HNSW batch is folded is in
`hnsw-batch-compaction.md`.

- **[V]** — holds in the code at commit `b91b359a3`, verified at the cited
  anchor. Every `file:line` in this document is against that commit.
- **[P]** — proposed: part of the target this design commits to, not yet in
  the code.

Anchors are `file:line`. Short names: `mod.rs`, `admission.rs`, `builder.rs`,
`replay.rs`, `state.rs`, `materialise.rs` and `tests.rs` are in
`surrealdb/core/src/kvs/index/`;
`ds.rs` is `surrealdb/core/src/kvs/ds.rs`; `executor.rs` is
`surrealdb/core/src/dbs/executor.rs`; `index_state.rs` is
`surrealdb/datastore/src/index_state.rs`; `schema.rs` is
`surrealdb/datastore/src/key/schema.rs`; `idx/…` is `surrealdb/idx/src/…`.

## The two concerns

**Build** is the durable protocol that takes one generation of an index to
`Online` [V]. Its state is table-scoped and shared by every node
(`mod.rs:9-21`): `!bs` holds the generation's phase, owner and scan
checkpoint; `!bt` issues writer tickets and `!br` reserves them; `!bg` queues
the mutations admitted writers make; `!bp` points a record at its first queued
mutation. The builder scans the table in checkpointed batches, replays `!bg`,
enters `Closing` to stop new admissions, replays again, waits out the
remaining reservations, replays a last time and publishes `Online`
(`builder.rs:2871-3435`).

**Materialise** is folding one kind's write-side state into the
read-optimised structure its queries use. It never changes the set of
records an index covers, which the write path decided, only how much work a
query does to answer it. An exact read (b-tree, FullText, Count) returns the
same answer either way. An approximate kNN search may differ only within the
index's normal recall (see below).

What the write path produces [V]:

| Kind | The write path produces | Folded into | By |
|---|---|---|---|
| `Idx`, `Uniq` | final entries (`idx/index.rs:231-262, 295-319`) | nothing: the entries are final | — |
| `FullText` | append-only deltas, per transaction: `!tx` per term, `!dx` for document stats (`idx/index.rs:498-539`, `schema.rs:710-746`) | `!td` bitmaps and the `!dc` root | materialiser (`Materialiser::process_fulltext_compaction`) |
| `Count` | one append-only signed delta `!iu` per transaction (`idx/index.rs:321-362`) | the `!iu` aggregate at the `None` bound | materialiser (`Materialiser::process_count_compaction`) |
| `Hnsw` | one record-keyed pending `!hr` per record, which later writes fold into (`idx/trees/hnsw/index.rs:436-482`) | the graph | materialiser (`Materialiser::process_hnsw_compaction`) |
| `DiskAnn` | record-keyed pendings in sharded `!dw`, each shard gated by `!dy` (`schema.rs:881-909`) | the graph | materialiser (`Materialiser::process_diskann_compaction`) |

The last four are the *queued kinds*. Full-text data may also hold `!tt` and
`!dc` deltas, an older shape that is read and drained but never written.
FullText compaction can also leave a `!tr` residual for a term: the part of
a document's total that a bitmap cannot hold. Queries fold it in with the
bitmap and the deltas (`idx/ft/fulltext.rs:107-115, 700-705`). A
write to a queued kind also queues one compaction request per index per
transaction under `!ic` (`idx/index.rs:545-573`), which is how the compactor
learns that an index has work.

Two rules connect the concerns, and both hold [V]:

1. **A query uses an index only when it is `Online`**
   (`index_state.rs:298-347`). Writers see every defined index, so that they
   can queue for one that is building.
2. **Every queued kind merges its backlog at read time.** A full-text read
   folds uncompacted term changes into the bitmaps it reads
   (`schema.rs:701-703`), a count sums every uncompacted `!iu`
   (`idx/index.rs:350-351`), and a kNN search scores pendings by hand beside
   the graph it walks (`index_state.rs:47-55`).

So **materialisation never changes the set of records an index covers; it
changes latency.** An exact read returns the same answer before and after it.
A kNN search scores unmaterialised vectors exhaustively and reaches
materialised ones by approximate traversal
(`idx/trees/hnsw/index.rs:1345-1553`, `idx/trees/diskann/index.rs:2262-2364`),
so its results may differ across materialisation only within the index's
normal approximate-nearest-neighbour recall. A large backlog turns an indexed
kNN search into something close to a scan. That is what makes the seam real.
Once a build is `Online`, every write that saw the definition, or whose
record the scan read, is in the index or in its write-side backlog, and
everything after that is materialisation. A write that did neither is a
separate race (see "Not covered here").

[V] The build ends at `Online` for every kind, the materialiser's passes are
the only code that folds a queue, and a blocking statement waits for both
(`Building::run_acquired`, `kvs/index/materialise.rs`).

## One build protocol, not one per index family

The split that suggests itself follows the families: b-tree kinds have no
queue and need the protocol, so let the queued kinds skip `!bg` and write
their own queues while the build runs. There is no per-family protocol to
split off [V]. Admission never looks at the kind. The builder branches on it
for the Count baseline (`builder.rs:2892, 3056`, `replay.rs:908`), the doc-ID
deferral for kinds that share the table's doc-ID space
(`replay.rs:1508-1544, 1718-1738`), the per-batch full-text context
(`replay.rs:626-627`), the value-shape tracking a cleaned b-tree index starts
(`builder.rs:2952-2968`), and whether a release owes a compaction request, which every queued kind does
(`Index::queues_compaction`, `builder.rs:2527`). None of these changes
the protocol's states or transitions. The split is also wrong for every family:

- **FullText and Count cannot skip it.** Their writes are deltas against the
  state the writer read. A writer that wrote its own delta for a record the
  scan then read would be counted twice, once in the scan's view of the new
  state and once in the delta. `!bp` fixes the baseline — the scan indexes the
  old state of a record's first queued mutation — and replay applies the
  queued mutations in ticket order on top of it (`replay.rs:147-158`). Count's
  merge of those baselines across checkpointed spans is the hardest part of
  the protocol (`replay.rs:91-211, 787-1253`).
- **Unique cannot skip it** for the same reason: an entry that a writer and
  then the scan both write for one record fails the uniqueness check against
  itself.
- **HNSW and DiskANN are the only kinds that could skip `!bg`**, because their
  pendings are record-keyed and fold: a later write keeps the first write's
  baseline and replaces the vectors (`idx/trees/hnsw/index.rs:436-482`).
  Skipping would still cost more than it saves:
  - The scan would start conflicting with writers. Writers queue under `!bg`
    keys that no scan batch writes; writing `!hr` or `!dw` themselves, they
    would collide with the batch that covers the same records.
  - The staged parallel HNSW apply would lose its empty baselines. A pending
    the scan writes has no old vectors, because nothing is in the graph yet,
    and a batch is staged only when no operation removes a vector
    (`idx/trees/hnsw/index.rs:666-674`). A writer's pending carries the
    vectors it replaced, so every batch holding one would fall back to the
    serial in-place path.
  - The admission check would stay anyway, because it is also the retirement
    fence: a writer holding a stale definition learns, from a read of `!bs`
    and the catalog in a transaction of its own, that the index was retired,
    and skips it rather than write under a prefix nothing maintains
    (`admission.rs:441-478`).
  - Ownership, takeover, the release gate and retirement would exist twice.

So there is one build protocol, and it ends at `Online`. Behaviour that is
genuinely per kind belongs in hooks inside it [P, optional]: the Count
baseline policy, the doc-ID deferral, the full-text batch context, and a sink
that lets an initial batch feed a kind's structure directly. Each hook takes
the key range its batch covers, so a scan later split by key range can call
the same hooks for each range without a second set of them.

## The release rule

`Online` with an owner is a window, not a state. `build_owner_holds` treats
a generation as owned while it is `Building` or `Closing`, and while it is
`Online` with its `owner` still set (`state.rs:141-181`) [V]. Only a released
owner, or one cleared after its lease lapsed (see "When the release never
comes"), lets compaction back into a vector index.

**Rule.** Ownership is released once the build is `Online` **and** the
transaction of the statement that started it has closed, by commit or by
rollback. [V] `Building::release_gate` is a rendezvous of the two halves
(`builder.rs:931-944`): the builder arrives when it stops writing
(`builder.rs:3484-3486`), the statement arrives from an action registered on
both outcomes of its transaction (`builder.rs:2134-2144, 3568-3611`), and
whichever arrives second releases; when the statement's half arrives first,
the publish itself commits the release (see "Released with the publish"). A build that no statement started — a
resume after a crash — starts with the statement half already arrived
(`builder.rs:1063`).

**Why the statement's close.** The statement stages the definition it builds
against in its own transaction. Until that transaction closes, every other
session reads the previous catalog, and the compactor is one of them. For a
`REBUILD INDEX` that migrates `format_version`, the previous catalog names the
retired layout: a compactor folding in that window would fold the new build's
pendings into the retired layout and leave the published index without them
(`materialise.rs:574-583`). Ownership is the one signal that is durable before the
definition is. After a commit the compactor reads the new definition; after a
rollback it reads the one it was already reading. Either way the fence has
nothing left to protect.

**Why only the vector kinds are fenced.**

- [V] The compactor defers to a live owner only for HNSW and DiskANN
  (`materialise.rs:215-226, 600-603`).
- A `REBUILD INDEX` keeps the index id and restamps the definition at the
  current `format_version` (`surrealdb/core/src/exec/vm/store.rs:1769-1777`).
  For FullText, that format selects only how records map to doc-IDs
  (`idx/ft/fulltext.rs:190-204, 453`), and compaction never consults the
  mapping. Count compaction reads no format at all. The keys a FullText or
  Count fold writes carry no format either.
- So folding a rebuild's deltas under the previous definition writes exactly
  what the new definition would fold. Both definitions name the same index
  id, so the same prefix. The rebuild's wipe clears that whole prefix,
  previous doc-ID mappings included, before its scan writes
  (`builder.rs:2898, 2956`, `idx/lib.rs:263`), so the deltas a fold meets
  carry doc-IDs from one space. A writer that indexes normally beside the
  rebuild is a separate hazard (see "Not covered here").
- A compactor running beside a build of the same index, as background
  compaction does for every FullText and Count build, is safe for the same
  reason two compactors are: the build's wipe advances the compaction
  generation, so no plan read before the wipe applies after it.
- A vector fold is different. It resolves doc-IDs and loads the graph
  through the definition's format, so only the build that knows the new
  layout may fold it.

[V] The fence covers exactly the kinds whose fold depends on the definition's
layout, and one list names them for both of its checks: the pass's dispatch
and the write guard's locked `!bs` read (`fenced_by_build_owner`,
`materialise.rs:215-226`). A format that made a FullText or Count fold depend on the
layout would add that kind to the list, until the release gate opens. Adding
a kind costs no work: a fenced pass keeps its request, and the release queues
one for every queued kind. What readers see while two layouts coexist is a separate
question (see "Not covered here").

**Why `Online`.** [V] The builder stops at `Online`, so publishing `Online`
is the builder's half of the gate: nothing it does afterwards writes the
index's data, so a compactor the release lets in has nothing of the build's
to race. Its half arrives only once the `Online` commit has applied, so a
statement that closes in between finds the generation `Online` when it
releases.

**Released with the publish [V].** When the statement half has already
arrived — the statement committed before the build read anything, or a
resume has no statement behind it — the publish clears the owner and stages
the release's compaction request in its own transaction, and no separate
release runs (`Building::mark_durable_online`). An index over the table's
doc-ID space is the exception: its builder runs the doc-ID reclaim sweep
after the publish, and releases only once the sweep has run, so no pass
resolves doc-IDs through mappings the sweep is reclaiming. It heartbeats the
owner while the sweep runs, however long that is, and stops the sweep if a
heartbeat finds the ownership gone (`Building::while_owning`).

**When the release never comes [V].** A statement whose node dies, a
statement whose transaction is dropped without closing, or a release whose
commit fails, leaves an `Online` owner that its normal release will not
clear.

- **The keeper.** While a build's statement is open past the publish, a keeper on its node
  heartbeats the owner (`Building::keep_online_ownership`). It stops once the statement closes,
  whose close releases. A transaction dropped without closing runs neither close action, so the
  keeper arrives in its place and releases. A heartbeat that finds the ownership gone — cleared, or
  the generation replaced — cancels the statement's transaction, as a second layer behind the fence
  below.
  A build that is aborted — by a removal, by a later build of the index, or by
  its statement's rollback — stops its keeper without writing, since whatever
  aborted it settles the ownership. A removal staged in the statement's own
  transaction stops it at once (`IndexBuilder::stop_build_of_statement`), so no
  heartbeat of `!bs` commits beside a transaction that deletes it, and a later
  build of the index replacing it on this node aborts it.
- **The lapse.** So a lapse of the owner's heartbeat means its keeper stopped. A lapsed owner is
  cleared by the compactor's write guard, in the transaction that applies a plan, and by the resume
  scan, beside the compaction request a release owes (`clear_lapsed_online_owner`,
  `Building::clear_lapsed_owner`). Dispatch lets a lapsed owner through to the guard, and the guard
  decides; a lapse alone never ends the fence (`build_owner_holds`).
- **The orphan.** A build whose defining statement never commits leaves build state, queues and
  index data under an id the catalog does not define, and the resume scan visits only defined
  indexes. A compaction pass that holds a request for that id, and reads a lapsed owner there,
  retires the build in one transaction (`retire_uncommitted_build`). It acts only while the
  index's name lookup is absent under a locked read. It fences the statement through the table's
  definition, rewritten or, where that statement defines the table too, deleted. It deletes the
  build state by compare-and-delete, then the queues and the data, and the pass drops the
  request. It is gated like the lapse.

**A lapsed heartbeat is not a closed transaction.** A paused or partitioned
keeper stops heartbeating, but its statement's transaction may still commit
later. If the owner were cleared on expiry alone, the compactor could fold
the new build's pendings with the previous catalog: for a `format_version`
rebuild, that is the retired layout. The transaction would then publish a
layout that is missing those updates. So whatever clears a lapsed `Online`
owner must also fence that transaction. One way is to make the statement's
commit conflict with the write that clears the owner, so that a transaction
outlived by its lease can only roll back. The other way is to clear only an
owner whose node is known not to be running, so that it can never commit.
Expiry alone never ends the fence.

[V] The clear takes the first way. The owner names a build task, not a node,
so the second is not available without a format change. Every `DEFINE INDEX`
and `REBUILD INDEX` takes a locked read of the table's definition and
rewrites it (`replace_table`), and so does the clear, unchanged. Of the clear
and a statement still open on the index, whichever commits second fails:
plain blind writes would not do, because TiKV and the TAPIR store
let two writers of one key both commit, while a locked read arms commit-time
validation on every backend the shared KV suite pins it on
(`getu_write_conflict`, `surrealdb/kvs-test/src/multi.rs`). The TAPIR store is
the exception that suite leaves out.

**Rolling upgrades [V].** The clear is sound only if every node that can own a
build stops writing at `Online` and keeps its owner's heartbeat while its
statement is open. A builder of an earlier release folds its own pendings
after `Online` without a heartbeat, so its lapse would not mean it had
stopped. Node records name their release (`NodeRelease`,
`surrealdb/catalog/src/node.rs`), and the store's feature level is the
cluster-wide record built on them: raising it is refused while a running node
records an older release or none (`Datastore::raise_feature_level`). So a
lapsed owner is cleared only on a store whose feature level is at least the
first release whose builders stop at `Online`, 4.0
(`lapsed_online_owners_clear`). A store held at `Legacy` keeps every `Online`
owner fencing until it releases or a `REBUILD INDEX` rewrites `!bs`.

- **What the operator owes.** The raise is a guard against a partial rollout, not a fence: no node
  of an earlier release joins after it. Development builds of the 4.0 line that predate this rule
  report 4.0 too, and are not a supported source for a rolling upgrade.

**One transaction [V].** The release and a compaction request for every
queued kind commit together (`builder.rs:2523-2607`). A pass turned away
while the definition was invisible — for the vector kinds by the ownership
fence, for FullText and Count because no committed definition was readable —
keeps one of the index's requests (prerequisite 4), but keeping a request
wakes nothing. The release's request, and the commit that carries it, wake
the compactor once the fence is down. Committed together, a pass that sees
the request also sees the release, and a release cannot commit without its
request. A release that does not commit queues nothing and leaves the owner
set.

## The materialiser

**Contract [V].** The compactor's per-index passes, one per queued kind, are
the only code that folds a queue. Two callers run them: the background
compaction task, which drains the `!ic` queue under a cluster-wide lease
(`Datastore::index_compaction`), and a blocking statement waiting on its own
index (`MaterialiseAfterCommit`). They live in
`surrealdb/core/src/kvs/index/materialise.rs`, behind a `Materialiser` built
from a `FrozenContext`, so a statement runs them without a `Datastore`
(`Materialiser::run_pass_held`, which also queues the request a pass owes);
`Datastore::index_compaction` keeps the queue, the lease and the rotation
across indexes.


**Two passes on one index are safe** [V], whoever runs them: two nodes, a
lease that lapsed mid-pass, or a waiter beside the background task. Every
queued kind keeps a compaction generation outside the range it folds: `!tv`
and `!dv` for FullText, `!iv` for Count, `!hg` for HNSW, `!dg` for DiskANN. A
plan records the generation it read, and its apply advances it with a
conditional write (`idx/lib.rs:138-177`), so of two plans read from one state
at most one applies (`idx/count.rs:206`, `idx/ft/fulltext.rs:1041, 1263`,
`idx/trees/hnsw/index.rs:654`, `idx/trees/diskann/index.rs:1643`). A plan
deletes only the exact keys it captured, so a write that lands after it is
left for the next plan. Wiping an index advances its generations rather than
deleting them, so no plan read before a wipe can apply after it
(`idx/lib.rs:205-288`). [V] A process-local mutex per index, taken by the
background task's per-index passes and by a waiter, keeps two passes in one
process from planning the same work (`IndexBuilder::materialise_lock`, owned
by the datastore and reached through the context's index builder). It goes to
its waiters in the order they queued: a waiter keeps one acquisition queued
across its deadline checks (`lock_unless`), so a background pass that queues
later does not get it first. It saves work; correctness rests on the
generation.

**A pass reports how it ended [V]** (`CompactionPassOutcome`): on an empty
plan; on a short plan, so the backlog it read is folded; on its budget, with
work left and requeued; on a lost race, because another compactor advanced
the generation first; fenced, because a live owner holds the index; or not
visible, because a live owner holds an index whose definition no other
session can read yet. The compactor's requeue and the batch's retention read
it, and so does the waiter's stop rule.

**Prerequisites.** What the passes must do before they can be the only
materialiser. All seven hold [V].

1. **Budgeted passes that requeue themselves [V].** A pass applies at most
   `INDEX_COMPACTION_PASS_PLAN_BUDGET` plans and, if work remains, queues a
   fresh `!ic` for its index before it returns
   (`Materialiser::run_pass_held`, `materialise.rs:165-174,
   414-427`). So one large index holds the queue batch that
   named it, and every other index in that batch, for at most that many of
   its plans. The request that led to the pass is deleted only after that,
   under the retention rule in item 4.
2. **The lease is kept while the per-index passes run [V].** It is renewed
   on a tick for as long as a batch's passes run, and while the batch awaits
   the passes still running after a failure (`Datastore::index_compaction_loop`,
   `ds.rs:6029-6087`). A pass that finds the lease lost stops at its next plan
   boundary and requeues its index.
3. **DiskANN retries a retryable conflict [V]**, as the other kinds do
   (`materialise.rs:1016-1039`), within the same retry budget,
   `INDEX_COMPACTION_PASS_MAX_RETRIES`.
4. **A retryable failure requeues [V].** A pass that fails on a retryable
   conflict it has no retry left for queues a fresh request before it
   reports, as a budgeted pass does.

   [V] **Retention rule** (`Datastore::index_compaction_batch`,
   `ds.rs:4739-4819`). A batch deletes the requests that led to a pass only
   after every pass in the batch has ended, and only after any fresh request
   a pass owed is durable:
   - A pass that ends on its budget, on a lost race, or on a retryable
     failure commits a fresh request before it reports. The batch then
     deletes the originals.
   - A pass whose fresh request fails to commit keeps every original for its
     index.
   - A fenced pass, or one that cannot read the definition yet, keeps one
     request for its index.
   - A pass that folded its backlog, or found its index removed, deletes them.
   - A node that dies before the cleanup deletes nothing, so the originals
     stay queued.

   A lapsed lease loses no wake-up either. Another node may process and
   delete an original while this node's partial apply is still uncommitted.
   But every apply advances its index's compaction generation, so a plan
   read before the other node's last apply cannot commit after it. A plan
   read after it folds only writes that came later, and each of those queued
   a request the other node's batch never read, so cannot delete.
5. **"Empty plan" is its own outcome [V].** Every apply reports an empty
   plan apart from a lost race (`CompactionApply`, `idx/lib.rs:179-203`,
   `idx/index.rs:380-384, 406-412, 471-475`), so a caller can stop on the one
   without mistaking the other for it.
6. **One write guard [V].** Every apply takes a locked read of its index's
   catalog definition inside its write transaction, so a retirement that
   commits between the plan and the commit conflicts the apply; the vector
   kinds take their locked `!bs` owner check in the same guard (see "Why only
   the vector kinds are fenced"; `compaction_write_guard`, `materialise.rs:228-281,
   718, 930, 1110, 1270`).

7. **A standalone residual is folded [V].** A compactor that knows the
   residual family writes a residual only for a term it captured in part,
   because the `!tt` scan was cut short before the term or the `!tx` page
   filled on it. A complete capture leaves a final total that the bitmap
   holds (`FullTextIndex::fold_term_docs`, `idx/ft/fulltext.rs`). The term's
   other deltas stay beside the residual, and the round reports more work,
   so a later round folds both. A compactor that predates the family folds
   the deltas into the bitmap and strands the residual. Queries still merge
   a stranded residual, and the term-docs plan also reaches it: after its
   delta scans, a round reads the `!tr` keys of the terms below its cutoff,
   where both delta scans passed, and captures each term that holds a
   residual and no delta in its snapshot. Such a term has no delta left, so
   its residual folds into `!td` as a term captured whole, and is deleted.
   The scan captures at most what the delta scans left of the round's
   budget, steps over the residual of a term whose deltas the round folds,
   which is folded with them, and looks one standalone residual further to
   learn whether a later round is owed
   (`FullTextIndex::prepare_term_docs_compaction_with_limit`).

   A stranded residual may also have lost its request, deleted by the
   compactor that stranded it, so it needs a wake-up. Whenever a drain of
   `Datastore::index_compaction` holding the lease ends with nothing left it
   can act on — the `!ic` queue empty, a batch that held the whole queue, or
   a lap over only the requests it kept — it sweeps the defined queued-kind
   indexes with the `compacting` probe (`index_has_unmaterialised_updates`,
   `!tr` included). It queues one request for each index that has
   unmaterialised state, no request queued already, and no pass in this
   drain — a pass that failed leaves its index to its next write — and
   drains them in
   the same call (`Datastore::sweep_unmaterialised_indexes`).
   - **Throttled.** A sweep runs at most once per lease period, measured
     from the last sweep that completed, and at most once per drain, so an
     idle store does not probe on every wake-up.
   - **Why it closes the ordering.** An older compactor can strand a residual
     and delete its request before an upgrade, after a lease handover, or
     from a legacy pass that outlived its lease. Whichever it is, the next
     empty-queue sweep on an upgraded holder finds the residual. A sweep run
     once at start would miss a residual stranded after it.
   - **Cost.** One existence probe per queued-kind index per lease period,
     and only while the queue is empty. A store with many indexes pays it
     while idle.
   - **No stopping signal yet.** The sweep could stop once no compactor that
     predates the family can run. Node records name their release
     (`NodeRelease`, `surrealdb/catalog/src/node.rs`), and the feature level
     records when every running node is at least a given release, so a
     store-wide signal exists; the sweep does not read it, because a residual
     stranded before the upgrade outlives the upgrade, and only a sweep finds
     it.

## Parallel work in a pass

A pass's parallel stages share the node with queries, so they draw on the
same workers and the same read model the query engine uses. Short names in
this section: `parallel.rs` is `surrealdb/idx/src/parallel.rs`; `places.rs`
and `steal.rs` are in `surrealdb/core/src/exec/vm/morsel/`; `snapshot.rs` is
`surrealdb/kvs/src/snapshot.rs`.

[V] HNSW's staged batch is the only parallel stage
(`idx/trees/hnsw/batch.rs:748-812`), and background compaction runs the
passes of a queue batch side by side (`Datastore::run_compaction_passes`).
Both take their workers and their reads as below.

[V] The query engine schedules its morsel workers from a node-wide place pool
with one place per core beyond the one a statement's own task runs on
(`places.rs:199-206`). A `Policy` caps a statement's places and keeps an OLTP
reserve free (`places.rs:111-153`). A freed place is owed to the waiting
statement that holds the fewest. A worker gives its place up at a quantum
boundary when another statement waits (`places.rs:162-171`,
`steal.rs:317-320`). The query engine reads through a `kvs::Snapshot`: one
transaction's read view, with range cursors that run concurrently and batched
point reads (`get_many`, `snapshot.rs:109-151`). Whether an engine gives one is
a property of the engine (`view_capabilities()`); one that does gives it only
for a transaction that has buffered no put or delete, and an engine that gives
none answers `None`.

**[V] One place budget.** Every materialiser worker runs in a place from the
node's pool, the one morsel workers use, under the same OLTP reserve and the
same fairness (`parallel.rs`, `idx/places.rs`):

- **Tickets.** A worker takes places through a scheduling ticket
  (`places::Ticket`). Background compaction holds one compaction ticket for a
  queue batch. It runs one index on its own task and starts another only
  when the ticket is granted a place, so places bound how many indexes
  compact at once (`Datastore::run_compaction_passes`). A build's own
  parallel work takes places through a build ticket that the builder
  registers on first use and drops when the build finishes
  (`Building::places`). When a blocking statement waits on the build, the
  statement lends the builder its registration instead, so the build's
  workers count as the statement's places (`exec/vm/store.rs:1859-1874`,
  `carried_places`). [V] A foreground pass, phase 2 of a blocking statement,
  carries that statement's registration the same way: the wait holds the
  lending context, and its passes take places through its ticket
  (`MaterialiseAfterCommit`).
- **Quantum.** A `run_jobs` worker holds one place across jobs rather than a
  permit per job. At every quantum boundary it checks whether another
  registration waits. If one does, the worker gives its place way and takes
  no more jobs. A registration that gave a place way asks for none while
  another waits, so it never takes back what it gave.
- **Progress without a place.** The calling task drains whatever jobs remain,
  so a run completes even when no place is ever granted. Results still come
  back in job order. A staged batch's graph is still a function of its base
  graph and its plan, whichever tasks ran its jobs, and whether places were
  granted, yielded mid-run or never granted.
- **Up-front grants.** Some work cannot take places one job at a time, because
  it is split across a fixed number of tasks before it starts and cannot drop
  a task part-way. A DiskANN frame is like this: its minibatch is partitioned
  statically across its tasks. Such work takes an up-front grant instead
  ([V] `Ticket::acquire_places`; [P] no such work takes one yet):
  - The grant never waits. It returns between none and the requested number
    of places, under the same reserve, cap and fairness as a per-job acquire.
  - The caller sizes the work from it: one task per granted place, plus the
    calling task. It never spawns a task that would then wait for a place.
  - The grant's lifetime is the quantum. Its places go back when the unit of
    work it covers ends, so each grant covers one bounded unit and the next
    unit asks again.
  - A grant of none runs the whole unit on the calling task, with the same
    result.
- **Mechanism.** How `surrealdb-idx` reaches the pool is not part of this
  contract. [V] It is a scheduler handle injected through `IndexEnv`
  (`IndexEnv::places`), over the pool's registration wrapped as a
  `PlaceSource` (`IndexPlaces`, `surrealdb/core/src/ctx/context.rs:2052-2094`).

**[V] One read view per batch.** A batch's workers read committed state through
one `kvs::Snapshot` (`parallel::ReadView`): `get_many` for graph point reads,
and [P] range cursors for the pending queues once a stage reads them in
parallel.

- **Where the view comes from.** The view comes from one read transaction
  opened for the batch's workers, whatever their number, and cancelled once
  the batch is built (`ReadView::open`). It cannot come from the apply
  transaction. By the time workers start, the apply transaction has buffered
  the generation bump, its conditional deletes and its allocations, and
  no engine gives a view over buffered writes. A view
  opened after another compactor committed belongs to a batch that will fail
  on the compaction generation, so nothing it reads is published.
- **Point reads must not serialise.** [V] On an engine that gives a view,
  `get_many` holds the transaction's slot lock at most to take a keep-alive
  and reads under it, as a cursor does, so point
  reads from many workers through one view run side by side.
- **No view, the calling transaction.** [V] Where the engine gives no view,
  the batch takes the staged path on the same workers, every one reading
  through the calling transaction, and opens no other transaction. It does not
  fall back to the in-place path. The graph is the same either way.
- **The trade-off.** mem, SurrealKV and TiKV give no view. Nothing writes
  through the calling transaction while the workers read, and mem and
  SurrealKV take its lock shared for a read, so their workers read side by
  side. An engine that runs one transaction's operations one at a time queues
  the workers' reads, while their distance work still runs in parallel.
  Correctness is the same on every engine.

A partitioned initial scan (see "Not covered here") needs the same two
properties. It also needs range boundaries that tile the table's record
prefix. The read view provides them (`Snapshot::split`, `snapshot.rs:176-208`),
along with cursors that give up their tail (`RangeCursor::narrow`,
`snapshot.rs:252-257`) and range stealing between workers (`steal.rs`).

## The blocking contract

**Contract [V].** A blocking `DEFINE INDEX` or `REBUILD INDEX` in a
transaction the executor owns returns once its index is `Online`, then
materialised. That holds for every kind, and whether this node runs the
build, takes it over, or waits on another node's: phase 2 follows phase 1
whichever of them phase 1 waited on. In a caller-owned transaction it returns
once its index is `Online`, and materialisation is left to background
compaction (see "Which transactions" below).


**Phase 1** waits for `Online`, and a build that fails fails the statement.
Where it runs:

- [V] A blocking `REBUILD INDEX` that is the whole of a transaction the
  executor owns runs phase 1 after that transaction commits
  (`surrealdb/core/src/dbs/top_level.rs:96-145`, `executor.rs:2996-3187,
  3218-3242`). A build that fails after the commit still fails the
  statement, and leaves the index in `Error` for the next `REBUILD INDEX`.
- [V] A blocking `DEFINE INDEX` that is the whole of such a transaction, on a
  table that existed before it, and that does not replace an existing index,
  runs phase 1 after the commit too (`exec/vm/store.rs:1910-1955`,
  `IndexBuilder::run_awaited_after_commit`). If its wait then ends in an
  error, it takes back only its own definition, and only while that
  definition's build is still the one it awaited and unpublished
  (`RemoveFailedDefinition`, `kvs/index/after_commit.rs:273-439`). The
  removal runs in a transaction of its own:
  - The catalog must still hold, under the index's name, the index id this
    statement published, on a table with the same table id. A replacement, a
    removal, or a table removed and defined again is no longer the
    statement's to undo, and is left alone.
  - The build state is read under a lock. A later generation means a later
    build has taken the index on, and the definition stays for that build to
    decide. The awaited generation at `Online` means the build published
    after all, and the definition stays and the statement succeeds. The lock
    makes a publish or a later build that commits while the removal is
    staged fail the removal's commit, and the retry reads what it wrote.
  - Only the awaited generation, not `Online`, or no build state at all, is
    removed with the definition. A definition with no build state would
    read as a ready index.
- [V] Every other blocking build runs phase 1 inside the statement's
  transaction: a replacement, which publishes the new index and retires the
  one it replaces in one commit, an index on a table the statement creates,
  and every blocking build in a transaction whose owner accepts no
  after-commit wait.

**Phase 2 [V]** waits for materialisation (`MaterialiseAfterCommit`, queued
by the build instruction behind phase 1 for every queued kind):

- **Only after commit.** The compactor reads the committed catalog: before
  the commit it cannot see a new definition and, for a `format_version`
  migration, would see the retired one. And materialisation carries no
  atomicity — it changes how the index is laid out, not what it answers — so
  nothing about it needs to commit or roll back with the statement.
- **The waiter drives its own index's pass**, under the process-local mutex,
  rather than waiting for background compaction. A passive wait would hang
  wherever no background task runs, which includes the test harnesses: the
  builder tests (`tests.rs:60`), the integration helpers
  (`surrealdb/core/tests/it/helpers/mod.rs:31`) and the language-test
  provisioner (`language-tests/src/cmd/run/provisioner.rs:99, 341`) all build
  datastores without maintenance tasks.
- **Stop rule:** the wait ends when a pass reports an empty or short plan. It
  does not wait for the backlog to be empty: writes that commit after the
  statement keep adding to it, and under a steady load that wait would not
  end. Within the waiter's deadline:
  - A pass that ends on its budget or on a lost race is followed by another
    pass.
  - So is a pass that ends on a retryable failure. That pass has already
    retried up to its own cap and requeued its index (prerequisite 4), and
    the waiter does not stop at it. On a datastore with no background task,
    nothing else would ever fold the index.
  - A fenced pass is retried after a pause, because a remote owner still
    holds the index. The owner releases once its build is `Online` and the
    statement that started it has closed, so a fence lasts as long as
    another statement stays open. A fence that outlasts the owner's lease,
    counted from the first fenced pass in a row, ends the wait as a deadline
    does: such an owner may never release, and with no query or transaction
    timeout the wait would otherwise not end.
  - A pass that finds no readable definition ends the wait with success: the
    index was removed or replaced after the commit, and nothing of the
    statement's is left to fold.
- **Deadline:** the wait is bounded by the statement's own deadlines, its
  query timeout and what remains of its transaction timeout. Reaching one is
  the only way the wait returns success with work still queued. The index is
  `Online` and correct, and its remaining backlog is merged at read time.
  Folding it is left to the queue: every write that left work left a
  request, and every budgeted or failed pass requeued what it left.
- **Errors.** Cancellation, or a non-retryable pass error, ends the wait
  early. It is reported as a post-commit error,
  `IndexMaterialiseWaitStopped`. That error says the transaction committed,
  the index is `Online`, and only the wait for materialisation stopped, with
  the backlog left queued for background compaction. It carries whether the
  wait was cancelled. A cancelled wait maps to the client's cancelled kind;
  any other stop maps to a plain query error. It is never a conflict, a
  timeout, or an unexecuted query, so nothing in it suggests running the
  statement again (`DatastoreError::IndexMaterialiseWaitStopped`).
- **A pass in progress is never abandoned.** At a deadline or a
  cancellation the waiter trips the pass's own canceller and awaits it, so
  the pass stops at its next plan boundary rather than mid-transaction.
- **It owes the commit nothing.** A phase-1 wait can take back what its
  commit applied, so an owner never commits a statement whose query has
  already ended, and runs that wait even after a commit of unknown outcome.
  The phase-2 wait compensates for nothing (`AfterCommitWait::compensates`),
  so an owner holding only it commits as it would with nothing queued, and
  skips it where nothing hears its outcome.
  - It is a sibling of main's after-commit build errors, not a reuse of
    them. [V] `IndexBuildWaitCancelled` and `IndexBuildOutlastedTimeout`
    report a build that is still running, in wording and wire forms pinned
    for that case
    (`surrealdb/datastore/src/error.rs:42-63, 307-317`). A materialisation
    wait ends with the build already `Online`, and a non-retryable pass
    error is not a cancellation, so neither error describes it.
- **Phase 2 never undoes and never fails a COMMIT.** The commit it follows
  stands and has no undo.
  - **`COMMIT` stays successful.** Once the commit has succeeded, the
    `COMMIT` slot of a `BEGIN…COMMIT` keeps its success value, `NONE`.
  - **The error goes on the statement's own slot.** A phase-2 error is
    reported on the slot of the `DEFINE INDEX` or `REBUILD INDEX` that queued
    the wait, whether it stands alone or sits in a block.
  - **A block resolves every wait.** A block queues one `(slot, wait)` pair
    per blocking index statement and resolves every pair, in order. It does
    not stop at the first wait that ends early, as the standalone path may:
    there, one statement owns the transaction and queues one wait.
    - A wait that stops writes `IndexMaterialiseWaitStopped` to its own
      statement's slot.
    - After a non-retryable error in one wait, the remaining waits still
      run, so later indexes still materialise or report their own error.
    - A cancellation is observed by every remaining wait. Each records
      `IndexMaterialiseWaitStopped`, marked cancelled, on its own slot, so
      no index statement whose index was not materialised reports `NONE`.
    - The deadline is shared. The block's waits draw on one budget, what is
      left of the transaction timeout alongside the query timeout, spent in
      the order they run. A wait that reaches it ends as completed, with its
      backlog left queued, and its slot keeps `NONE`, since reaching the
      deadline returns success. Waits after the deadline return at once on
      the same terms, as deadline-reached rather than stopped.
  - **Every other slot.** Every queued wait is resolved onto its own
    statement's slot, `COMMIT` stays `NONE`, and every slot with no stopped
    wait keeps its committed value.
  - **Telemetry follows the slot [V].** A standalone statement settles its
    after-commit wait before its statement event is emitted and its result
    recorded. A block emits each statement's event as soon as it has run,
    except a statement that queued a wait: it holds its event with its
    `(slot, wait)` pair, and emits it once that wait settles, so the event's
    outcome, error class and duration agree with the slot
    (`Executor::settle_block_waits`). A block that ends before phase 2 (a
    `CANCEL`, a failed commit, a later statement's error, or a timeout before
    the commit) emits each held event with its phase-1 outcome before it
    returns. Telemetry for a block that does not commit is otherwise
    unchanged.
  - [V] A standalone statement's own transaction holds only that statement,
    so the error a wait returns becomes that statement's result after the
    commit. A phase-1 wait raises the build's own error, a timeout or a
    cancellation, never a commit failure (`kvs/index/after_commit.rs`). A
    block pairs each wait with its statement's slot in the same way: it
    counts the waits each statement queued, and resolves them in order once
    it has committed.
- **Which transactions [V].** Every write transaction the executor owns runs
  phase 2: a statement's own transaction, whatever the statement holds,
  since a blocking index statement can be reached through a block, an `IF`
  or a function body (`Executor::execute_in_own_transaction`); and the
  `COMMIT` of `BEGIN…COMMIT`, which returns once every index the block built
  is materialised (`Executor::execute_begin_statement_impl`). A transaction
  says separately whether it accepts a deferred phase 1
  (`Transaction::accepts_after_commit_waits`, a statement that is the whole
  of its transaction) and a phase 2
  (`Transaction::accepts_materialise_waits`, every transaction above):
  `BEGIN…COMMIT` runs phase 1 inside its transaction, so that it publishes a
  definition only with a finished build, and accepting phase 2 does not
  defer it.

  A caller-owned transaction runs phase 1 only. That is a transaction an RPC
  client opens, runs statements in (`ds.rs:6769`, `executor.rs:4123-4263`)
  and commits itself.
  - A statement in it returns while the transaction is still open and
    uncommitted. Phase 2 runs only after a commit, so it cannot run before
    the statement returns.
  - [V] Phase 2 after such a commit would need the transaction's owner to
    accept after-commit waits and await them once its commit has succeeded
    (`surrealdb/datastore/src/close.rs:69-133`). No session commit does: the
    WebSocket, gRPC and embedded engines commit with no queue
    (`surrealdb/server/src/rpc/websocket.rs:1538`,
    `surrealdb/server/src/rpc/grpc.rs:883`,
    `surrealdb/engine-local/src/engine.rs:670`).
  - [V] They are left so. A caller-owned index is materialised by
    background compaction. On a datastore with no background task, such a
    caller gets an `Online` and correct index whose backlog is merged at
    read time and waits for maintenance to fold it.
- **What the defining transaction reads.** A blocking build that runs inside
  the transaction (the last bullet of phase 1) reads and writes in
  transactions of its own, so its entries are committed past the snapshot of
  the transaction that defined the index, and a `CONCURRENTLY` build writes
  none until that transaction has closed. The transaction records every new
  index of that kind it cannot read
  (`surrealdb/datastore/src/unseen_indexes.rs`), unless a blocking build found
  no record of the table to index:
  - Its reads go around the index. `filter_online_indexes` leaves the index
    out for that transaction, and what is compiled from that answer is kept
    for no other. A full-text match, which only the index can answer, finds
    none to run on until the commit, as over an index still building. A
    nearest-neighbour search ranks the rows of the table, as it does while a
    vector index builds.
  - A write that adds a `UNIQUE` entry checks its key through a view opened
    once the build had returned, which reads what the build wrote: the
    transaction's own read of the key would not. A duplicate of a committed
    value is a uniqueness error where it is written, not a write conflict at
    `COMMIT` that a retry meets again.
  - A write that changes what the build wrote for a record, an update or a
    delete of a row that existed when the index was built, is refused
    (`EntryOfUnseenBuild`): writing the key fails the commit against the
    build's own write however often the transaction is retried. A row the
    transaction creates after the definition is its own, and it changes it as
    it likes. A row it wrote before the definition is neither: the build never
    saw it, so the index does not hold it, yet nothing here tells it from a
    row the build indexed, and a change to it is refused too. A count index
    keeps nothing of a row, so every change to one is written.
  - A build over a table that held no record is built with nothing to write,
    and its index is read and written as any other. It lists the rows the
    transaction writes after the definition, and no other: a row the
    transaction wrote before it, or one its snapshot holds that another
    transaction has since deleted, has no entry, and an indexed read leaves it
    out. The transaction's own view of the table cannot say which of the two
    it holds, since it shows what the transaction wrote together with its
    snapshot's.
  - A `REBUILD INDEX` inside a larger transaction is not recorded: its build
    runs in transactions of its own as well, and a statement of the
    transaction that then changes an indexed value of a row the rebuild wrote
    again fails the commit with a write conflict. The rules above are for a
    `DEFINE INDEX`, which has no entries before its build.
- **Outside the transaction timer.**
  - [V] A statement in its own transaction already runs its after-commit work
    outside the timer. It executes under the transaction timeout, commits,
    and only then awaits that work, with what is left of the timeout as its
    budget (`executor.rs:3033-3052, 3116-3176`).
  - [V] A `BEGIN…COMMIT` block runs under the timeout as a whole, `COMMIT`
    included. When the timer fires, it cancels the transaction and rewrites
    every slot of the block as timed out (`executor.rs:3469-3532`).
  - [V] Inside that wrapper, a phase 2 that outlasted the timeout after a
    successful commit would report a committed block as unexecuted, and a
    client could retry it unsafely. So a block's phase 2 runs after the
    block's timer has ended, outside that rewrite, as a single statement's
    after-commit work does. What is left of the transaction timeout since
    the block began becomes the waiter's deadline, alongside the query
    timeout. Reaching it stops the wait, and the block returns committed.
  - [V] Cancellation, or a non-retryable pass error, appears only on the
    slot of the index statement whose wait it ended, as the post-commit
    error above. Every queued wait is resolved onto its own statement's slot,
    `COMMIT` stays `NONE`, every slot with no stopped wait keeps its
    committed value, and no slot is rewritten as timed out once the commit
    has succeeded.

## Status

[V] `INFO FOR INDEX` reports `building.compacting`: whether the index still
has updates waiting to be folded, as existence, never depth
(`index_has_unmaterialised_updates`, `index_state.rs`). Every queued kind is
probed over the ranges its compaction folds, and never over the keys a
compaction writes as its result. HNSW probes its pendings and DiskANN its
shard guards. FullText probes `!tx` and `!dx`, the delta ranges of the older
`!tt` and `!dc` shapes, which exclude the keys compaction writes at their
bounds, and the `!tr` residuals, so `compacting` is true while any residual
is present. Count probes for an `!iu` entry with a `uid`; the aggregate at
the `None` bound sorts first and is not a backlog (`schema.rs`). A b-tree
kind reports `false`.

[V] A blocking statement that returns success has left `compacting` false,
unless it returned on a deadline or writes arrived after its last pass. One
that returns an error after its commit leaves the index `Online`, with its
backlog queued.

Progress counts are out of scope: `compacting` stays an existence test, since
a count is a scan as long as the backlog it counts.

## Invariants

- **No step changes the set of records an index covers** relative to the
  write-side state, and the `Online` gate holds. Exact reads keep their
  answers; approximate kNN results may differ only within normal recall.
  The writer-snapshot races that predate this design are listed under "Not
  covered here".
- **No persisted format changes.** `IndexBuildState` stays at revision 4
  (`surrealdb/datastore/src/values/index_build.rs:108`); no key family is
  added, and none changes shape. What the target needs durably — ownership
  and the owner's heartbeat — already has a field.
- **The build fences do not change**: the `Building`/`Closing` fence, and the
  retirement fence, in which the schema transaction deletes `!bs` and every
  build write compare-and-swaps it first, at three sites —
  `transition_owned_build_state`, the one loop every owned transition takes,
  the release and the end of a stopped build included;
  `update_build_state_in_tx`; and `maintain_build_ownership`
  (`builder.rs:1610-1772, 1857-1883, 2705-2733`).
- **No new class of durable `Error`**, and a failure after `Online` never
  demotes a queryable index. [V] `mark_durable_error` and
  `mark_durable_aborted` leave an `Online` generation as it is
  (`Building::mark_durable_ended`, `builder.rs:2609-2693`).
- **No wall-clock ratio tests.** Structure is pinned by exact-count
  accounting.

## Test strategy

- **Failing test first.** Each gap a [V] note above describes gets a test
  that fails against the current code, landed with the change that closes it.
- **Exact counts, not timings.** Backlogs are counted by key (`!hr`, `!dw`,
  `!tx`/`!dx`, `!iu` with a `uid`, `!ic`), conflicts and failures by the
  per-site injection counters (`surrealdb/datastore/src/testing.rs:26-141`).
- **The statement window.** The release rule is pinned by a build inside a
  statement transaction the test holds open (`tests.rs:8014-8036`), asserting
  ownership, the queued request and the compactor before and after a commit,
  and after a rollback.
- **Two passes, per kind.** Two passes planned from one state on one index:
  one applies, the other reports a lost race, and every delta or pending is
  folded exactly once.
- **No background tasks.** Phase 2 is proven on datastores without
  maintenance tasks, where only the waiter's pass can fold anything; holding
  the per-index mutex proves a statement does not return before its pass, for
  a local and for a remote owner.
- **A post-commit error on its own slot.** In `BEGIN; DEFINE INDEX …;
  COMMIT;` with phase 2 cancelled, `COMMIT` is `NONE` and the `DEFINE` slot
  carries `IndexMaterialiseWaitStopped`, mapped to the cancelled kind. A
  non-retryable pass error lands on the same slot as a plain query error.
- **An event matching its slot.** In a block whose phase-2 wait is
  cancelled, the `DEFINE` statement's event reports the same outcome and
  error class as its slot, not the success it had before the commit.
- **Several waits in one block.** In a block with two blocking
  `DEFINE INDEX` statements, a non-retryable error injected into the first
  wait leaves the first slot carrying the error. The second index still
  materialises, its slot is `NONE`, and so is `COMMIT`. Under cancellation,
  both index slots carry the cancelled error and `COMMIT` is `NONE`.
- **A block outlasting its timer.** A `BEGIN…COMMIT` block whose phase 2
  outlasts what is left of the transaction timeout returns its committed
  results, `COMMIT` included, and no slot reports a timeout.
- **A stranded residual.** A `!tr` residual written with no delta beside it
  makes `compacting` true, and one round folds it into `!td` and deletes it.
  On an already-running datastore, with no delta and no `!ic` queued, the
  next `Datastore::index_compaction` discovers it through the empty-queue
  sweep and folds it, and `compacting` ends false.
- **A retryable failure is not the end.** On a datastore with no background
  task, a pass that fails once with a retryable error, injected past its own
  retries, is followed by another. The statement still returns with the index
  materialised and `compacting: false`. A non-retryable error injected into a
  pass ends the statement with that error, after a commit that stands.
- **Places, counted on the pool's books.** No materialiser worker takes a
  place from the OLTP reserve. A worker that finds another statement waiting
  at a quantum boundary gives its place back. A batch run with places granted,
  yielded mid-run or never granted produces the same graph.
- **Registered reads, on a backend that validates them.** The write-guard
  tests commit a retirement between the guard's read and the apply's commit,
  on an engine that validates registered reads, for every queued kind
  (`assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it`,
  `ds.rs`).
- **Language tests.** A deterministic reproduction that a blocking
  `DEFINE INDEX` returns with `compacting: false` for each queued kind; the
  existing `compacting` expectations keep their values.

## Not covered here

- **Writes missed while a definition is invisible to other sessions.**
  A writer takes the table's index list from its own snapshot. A writer that
  cannot see a definition never consults admission, so it neither indexes nor
  queues for it.
  - This is orthogonal to the build and materialise split. It is closed by
    what a build waits for before it reads, not by `!bg` replay or by
    materialisation.
  - A build that reads once the definition has committed waits for the
    statement's transaction to close, so that writers which begin after the
    commit see the definition (`Building::await_statement_closed`), and then
    for every write transaction older than the commit
    (`WriteBarrier`, `kvs/write_barrier.rs`, counting the node's own
    transactions through `LiveWrites`, `datastore/src/triggers.rs`). A writer
    older than the commit commits and is indexed exactly once: it maintains
    the index as its snapshot shows it, and the build reads past it. Every
    `CONCURRENTLY` build is of this kind, and so is a blocking build awaited
    after its statement commits.
  - A blocking build inside `BEGIN…COMMIT` is awaited inside the transaction
    and reads before the definition has committed. It waits for the writers
    older than itself, but writers that begin after it started waiting cannot
    see the definition either, and it cannot wait for them. They are held
    back instead: from the moment the build begins waiting
    (`LiveWrites::advance_holding`), a write through the same datastore that
    reaches a record of the table waits for the transaction to end
    (`DefinitionHold`, `WriteCtx::wait_for_definitions`) and is refused with a
    retryable conflict if the transaction committed, since its snapshot lacks
    the index; its retry sees the index. The defining transaction is not held
    back, and neither are the maintenance transactions that write under the
    table without writing its records, such as a build's, which never pass
    through the record write path. A write that waits for longer than
    `DEFINITION_HOLD_WAIT` is refused as well, so an open transaction holds a
    table's writes back for a bounded time.
  - Still open: the hold is kept in memory by the datastore that runs the
    definition. On a store several nodes write, a write through another node
    that begins after the build started waiting is not held back, and a
    blocking build inside `BEGIN…COMMIT` can miss it. Covering it takes a
    persisted entry that the nodes answering the write barrier watch.
- **`REBUILD INDEX` racing writers that index normally.** [V] A writer that
  finds the index `Online` holds no reservation, so a delta it writes can
  outlive the rebuild's wipe. This is the same kind of race as the one
  above, with the same kind of fix.
- **Atomicity of a `REBUILD INDEX` that migrates `format_version`**: what
  readers, a cancelled statement and a crash-resume see while two layouts
  coexist.
- **The per-mutation cost of admission.** Admission opens a transaction for
  every indexed mutation, even with no build in flight; caching its answer
  safely depends on fencing writers whose snapshot predates a definition.
- **A parallel initial scan.** Its substrate exists: `Snapshot::split` cuts the
  record prefix into ranges, `RangeCursor::narrow` lets a running range give up
  its tail, and workers steal ranges from each other. Its places and reads
  follow "Parallel work in a pass", and the kind hooks take a key range so the
  scan can feed them range by range. What remains is the build side:
  per-range checkpoints, per-range fences and a coordinator heartbeat, with
  Count's span merge the hard part. One protocol makes that neither harder
  nor easier.
- **Parallel index writes from one statement.** One statement can prepare a
  bulk write's index keys in parallel and apply them in row order. That
  raises the same admission and missed-write questions as any other writer,
  and changes neither the build contract nor the materialise contract.
