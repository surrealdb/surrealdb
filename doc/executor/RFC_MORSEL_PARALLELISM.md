# RFC: Morsel-driven parallelism in the VM

**Status:** phases 1 to 5 implemented (`exec/vm/morsel/`, `exec/planner/placement.rs`,
`exec/vm/place.rs`); phases 6 to 8
are the plan of §16. A read statement runs on several cores of one node: its input is cut into
morsels, workers take morsels from the statement, and the statement merges what the morsels
produce into exactly the answer the serial run gives. Morsels are the one unit of intra-query
parallelism. The first target is single-node SurrealKV;
kv-mem implements the same read-view capability next, and surrealds is the backend
the KV contract is written for. The unit is shaped so that a morsel later ships to an edge leaf
running on surrealds without changing what it means.

§17 lists the decisions this RFC states.

Companions: [`PLAN_LAYERS.md`](PLAN_LAYERS.md), which states what the logical plan, the physical
plan, lowering and the VM each decide, and so which layer decides what this note describes;
[`EDGE_PLAN.md`](EDGE_PLAN.md) (W6, rules 6, 7 and 10), whose morsels, exchanges and
KV contract this note makes concrete for one node;
[`RFC_LOGICAL_CONTROL_FLOW.md`](RFC_LOGICAL_CONTROL_FLOW.md) §5.5, whose read frontier names the
ranges a request reads; [`VM_MEMORY.md`](VM_MEMORY.md), whose governor and leases every morsel
charges; [`VM_LEAF.md`](VM_LEAF.md), whose leaf profile decides which morsels can leave the node;
[`RFC_STATISTICS_AND_COST.md`](RFC_STATISTICS_AND_COST.md), whose estimates refine morsel sizing
and never decide correctness. `EDGE_PLAN.md`, `RFC_LOGICAL_CONTROL_FLOW.md`, the VM pre-emption
components (`vm/fuel.rs`, `exec/scheduler.rs`, `vm/sort.rs` and the runtime's loser tree,
`Transactable::count_bounded`, `parallel_count` and `count_cost`, `exec/operators/scan/extents.rs`)
and the bounded-memory components (`exec/memory`: governor, leases, temporary store, runs) are
proposed on their own branches; where a phase depends on one, §16 says so.

Code paths are relative to `surrealdb/core/src/` unless another crate is named.

---

## 1. Goal and invariants

A read statement whose pipeline is morsel-eligible (§7) runs on as many cores as the node's worker
budget gives it, and on one core when the budget gives none. Its answer is the serial run's answer.

The invariants every part of this design upholds:

1. **The serial run is what a morsel run means.** Rows, row order, errors and the response
   stream items equal what the statement gives when one task reads the whole range. Where the
   serial order is unspecified by the language but observable on `main` (a scan's key order, a
   sort's tie order, the order of groups), the morsel run gives `main`'s order.
2. **One snapshot.** Every morsel of a statement reads the snapshot the statement's transaction
   reads, as of the statement's `VERSION` when it has one.
3. **The program is compiled once.** Splitting, stealing and merging are host decisions over a
   compiled pipeline. No morsel plans, lowers or rewrites bytecode; the physical plan decides
   eligibility when the program is compiled and the host decides width while it runs.
4. **No allocation on the read path.** A morsel over a key range reads its own range through a
   cursor whose buffers it reuses, and runs the pipeline in place on the stored bytes. Nothing on
   the way from storage to the body is cloned, decoded into an owned value or collected to be
   handed to another thread; what a body emits is decoded once, where it leaves the morsel.
5. **Bounded memory.** Every buffer a morsel holds is charged to the query's lease. Output a later
   morsel produces ahead of the merge is held in a fixed window; a full window parks the morsel.
6. **Pre-emption is the VM's.** A worker runs under the fuel meter and yields only at the VM's
   yield point. Fairness between queries is decided at morsel and quantum boundaries, never by
   scattered yields.
7. **Width never changes the answer.** A run with one morsel, with a morsel per key, and with
   any split or steal in between gives the same answer. The test lanes in §16 run all three.

## 2. What exists

### 2.1 Batches input (`exec/vm/morsel/set.rs`)

Every pipeline whose rows the coordinator reads and hands on in decoded batches — index, lookup,
index-union, bitmap, full-text, KNN and lightweight-edge sources, unions of them, and any storage
source whose body is not in place — runs as morsels of the **Batches** input. The coordinator, the
statement's task, reads the source and fills a batch; a batch runs on the statement's task or on a
tokio task holding a place in the node's worker pool (`morsel/places.rs`: a semaphore of
`available_parallelism − 1` places). The merge takes batches in the order the source yielded them,
so the output is the batches' rows concatenated, a stable sort receives entries in serial order,
and an aggregate folds partial groups in batch order. A failure is the earliest batch's, raised
where the serial run raises it. A body that pages its rows to the output runs each batch as though
every row were taken, but for the rows `START` is sure to skip, and the merge replays the paging;
once the limit is spent the batches still out are dropped and their workers aborted, and a failure
past the limit is never raised.

Which pipelines run this way is `Policy::plan`'s decision (§7), made once when the physical plan is
placed. The Range input of §5 is the same merge over parts of a key range that workers read
themselves; the two share `MorselResult`, the paging replay and the absorption of breakers
(`morsel/merge.rs`).

### 2.2 Counts in parts

`Transactable::count_bounded` (count at most a budget of keys, answer the key to resume at),
`Transactable::parallel_count` (the backend counts off the calling thread), `count_cost` (linear,
or logarithmic for a backend that keeps subtree counts) and `split_range` in the `kvs` crate
(`kvs/src/split.rs`: divide `[lo, hi)` into disjoint extents on the bytes that distinguish the bounds)
already count one range as several in-flight parts against one snapshot. That is the morsel pattern restricted to counting; §15 names how it joins this design.

The `range_shape` tests of `split_range` pin a fact the splitter in §5 depends on: dividing a table's
full key bounds balances badly, because generated ids occupy a narrow band of the byte space;
dividing the band the keys occupy, found by a two-seek probe, balances well.

### 2.3 What each backend allows

| backend | concurrent reads of one transaction | owned read view (`Transactable::snapshot`) |
|---|---|---|
| SurrealKV | Scans take the inner `RwLock<Tx>` for read, so cursors of one transaction read concurrently. | Yes, for a read-only transaction; a writable transaction answers `None`. |
| kv-mem | Scans take the inner lock for read; cursors resume by bound. | `None` today; same. |
| TiKV | Every read takes the inner `RwLock` for write, so reads of one transaction are serial. | `None`; not a priority. |
| surrealds | No range-scan or cursor API yet. | To be designed: §4 is the contract it implements. |

A backend that answers `None` is correct: the coordinator still cuts a scan into morsels and runs
them itself, in order, through the transaction's own cursor, so the answer is the one a backend
with a view gives, at width one.

## 3. The morsel

A **morsel** is one unit of a statement's input:

- **a pipeline**: the compiled pipeline it runs, named by program and index (on a leaf, by block
  identity, §14);
- **an input**: either a half-open key range `[lo, hi)` within the keyspace the pipeline's source
  reads, with the source's direction (**Range**), or a batch of decoded rows the coordinator read
  (**Batches**);
- **a snapshot**: for a Range morsel, the read view every morsel of the statement shares (§4.1), as
  of the statement's version;
- **an order key**: for a Range morsel, the range's start in scan direction — `lo` for a forward
  scan, `hi` for a backward one. Morsels of one statement are disjoint, so order keys are distinct,
  and ordering morsels by order key in scan direction is the order the serial run reads their rows
  in. Batches are ordered as the coordinator filled them;
- **its state**: a `State` of its own, made from the statement's (`State::sharing`, the statement's
  frames, slots and permission bypass), holding the partial state of every breaker the body fills:
  rows for the output, a sort's entries, an aggregate's partial groups, and for a paged body how
  each row ended;
- **its position**: the first key it has not yet visited. Everything before the position has been
  run; `[position, end)` is the morsel's remainder and can be handed to another worker.

What a morsel produces is a **morsel result** (`MorselResult`): its output rows, its breakers'
partial state once its range is exhausted, how each row ended for a paged body, and at most one
failure at the row it occurred at. A morsel ends by finishing its range, by parking, or by being
cancelled.

A queued Range morsel is plain data: range and order key. It holds no `ExecutionContext` and no
borrowed cursor while it is queued, so it can be parked, split, moved to another thread, and later
encoded for a leaf without change.

## 4. The KV contract

The contract is written for surrealds first: it is the backend this design is shaped for, and the
others implement it as adapters. It lives in `surrealdb/kvs/src/{snapshot,page,split}.rs` and
extends `Transactable` (`api.rs`); every addition has a default, so a backend that implements none
of it runs every statement correctly, at width one.

### 4.1 An owned read view

```rust
/// An owned, shareable read view of one transaction, taken at one moment.
pub trait Snapshot: Send + Sync + 'static {
    fn open<'a>(&'a self, rng: KeyRange<'a>, dir: Direction, version: Option<u64>)
        -> BoxFut<'a, Result<Box<dyn RangeCursor + 'a>>>;
    fn estimate<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>)
        -> BoxFut<'a, Result<Estimate>>;
    fn split<'a>(&'a self, rng: KeyRange<'a>, target: SplitTarget, version: Option<u64>)
        -> BoxFut<'a, Result<Vec<Key<'static>>>>;
    fn count_bounded<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>, budget: u32)
        -> BoxFut<'a, Result<(usize, Option<Key<'static>>)>>;
}

pub trait Transactable {
    /// This transaction's owned read view, or `None` when the backend cannot give one now.
    fn snapshot(&self) -> Option<Arc<dyn Snapshot>> { None }
    fn estimate(...) -> ...;   // default: through `scan`
    fn split(...) -> ...;      // default: two seeks and `split_range`
}
```

Contract:

- **Same view.** Every read through the view sees exactly what the transaction read at the moment
  `snapshot()` was called, at the same version: committed data at the transaction's snapshot, plus
  the writes the transaction had buffered then. A commit by another transaction, and a write the
  transaction makes later, are invisible to it. A backend that cannot overlay buffered writes
  answers `None` whenever any are buffered, so a read-only statement inside a writable transaction
  parallelises once a backend can freeze its overlay, and sees its own earlier writes. Today a view
  is given only for a read-only transaction.
- **Statements that write stay serial.** The engine plans a pipeline as morsels only when its
  statement may not write (§7), so what the transaction has buffered cannot change while morsels
  run.
- **Keep-alive.** The view keeps the snapshot alive. A transaction's commit or cancel waits until
  every cursor opened from the view is dropped, and once the transaction has finished `open` fails
  with `TransactionFinished`. The statement drops its cursors before it returns (§11), so the wait
  is bounded by one chunk.
- **Concurrent.** Cursors opened from one view advance concurrently on different threads with no
  lock shared between them on the read path. Where a backend's blocking iterator belongs is the
  backend's to say: a backend with blocking I/O runs every chunk of a view's cursor on the affinity pool, off the
  tokio workers.

Per backend: SurrealKV builds the view for a read-only transaction. kv-mem implements the same
capability next. surrealds implements the trait natively.
TiKV uses the defaults and is not a priority: no region-aligned splitting and no client change.

### 4.2 A cursor that is re-aimed and fills caller-owned pages

```rust
pub trait RangeCursor: Send {
    /// Point the cursor at a new range, keeping its iterator and buffers. Never reads.
    fn aim(&mut self, rng: KeyRange<'_>, dir: Direction) -> Result<()>;
    /// Shrink the range still to be read without re-seeking.
    fn narrow(&mut self, end: Key<'_>) -> Result<()>;
    /// Visit up to `limit` entries in place (the zero-copy path the VM runs its in-place
    /// pipelines through).
    fn for_each<'s>(&'s mut self, limit: u32, f: &'s mut dyn ValVisitor)
        -> BoxFut<'s, Result<ScanChunkStats>>;
    /// Copy up to `limit` entries, and no more than the page holds, into `page`.
    fn fill<'s>(&'s mut self, page: &'s mut Page, limit: u32)
        -> BoxFut<'s, Result<ScanChunkStats>>;
    /// Where the cursor would continue, or `None` once the range is exhausted.
    fn resume(&self) -> Option<Resume>;
}
```

- **Re-aimable.** `aim` reuses the cursor's iterator and buffers. A worker holds one cursor for its
  lifetime and re-aims it at each morsel it takes, so taking a morsel allocates nothing once the
  worker has warmed.
- **Caller-owned pages.** A `Page` is a key buffer, a value buffer and an offsets table
  (`KeyValSpan`), sized once. Its layout is the batch format of `EDGE_PLAN.md` rule 7 —
  length-prefixed entries with an offsets table, stored bytes untouched — so a page a leaf fills is
  the page it ships. An entry larger than the page is delivered alone in a page grown for it.
- **Resumable.** `resume` after any chunk names an opaque position such that reading
  `Resume::remainder` next visits exactly the entries not yet visited. A morsel's remainder is
  always expressible as a range, which is what parking (§8.1), stealing (§5.3) and leaf retry (§14)
  rely on.
- **Bounded chunks.** A chunk visits no more than `limit` entries, and the VM passes no more than
  the fuel left (`vm::fuel::batch`), so a chunk overshoots the quantum by at most one read.

The transaction-bound cursor (`ScanCursorVals`) remains what the coordinator reads through;
`RangeCursor` is its view-bound counterpart with re-aiming and positions. A backend implements both
over one iterator type.

### 4.3 Estimate and split

```rust
pub struct Estimate { pub keys: u64, pub bytes: u64, pub exact: bool }
pub enum SplitTarget { Parts(u32), Bytes(u64) }
```

- **`estimate(range)`** answers approximate keys and bytes. `exact` is true only when the numbers
  are exact at the snapshot (a backend that keeps subtree or page counts). The default reads up to
  `ESTIMATE_BUDGET` entries and extrapolates when the range does not end within them. An estimate
  shapes morsel sizes and never decides an answer.
- **`split(range, target)`** answers boundary keys strictly inside `range`, strictly ascending,
  dividing it into about `target` parts or parts of about `target` bytes, at most
  `MAX_SPLIT_PARTS`. Boundaries align with the storage's own units where it has them — page files on surrealds — so a morsel reads whole storage units.
- **Record-aligned.** Every boundary is a key at which a reader can start without reading part of
  a record before it. Today a record is one key, so every key qualifies; a layout that stores one
  record under several keys (chunked, PAX, columnar segments) must answer only boundaries between
  records, so no morsel ever starts inside one.
- **May answer fewer.** A backend may answer fewer boundaries than asked, including none; the
  caller then has fewer, larger morsels. A boundary divides the byte space the keys occupy, not
  the keys, so a part may hold no entry.

The default `split` is two seeks — the first key at or after `lo` and the last key before `hi` —
followed by `split_range` over that occupied band, which the `range_shape` tests show balances
generated ids. Backends use the defaults today; answering `estimate` from approximate sizes and
`split` from the storage's own key ranges is a follow-up.

## 5. Splitting

### 5.1 Start serial, split once the scan proves large

A pipeline planned as Range morsels starts as one morsel over its whole range, run by the
statement's own task (the **coordinator**) directly on the statement's state, reading through the
transaction's own cursor and emitting as the serial run does. After each chunk the coordinator asks
`Policy::split`. The policy answers `Split` once the coordinator has visited `morsel_split_rows`
entries (default 256), the transaction gives workers a read view to read through (unless
`morsel_needs_view` is `false`), and what is left of the range fills at least two morsels.

What is left is measured from what the coordinator has read. One read of the last key the range
holds, made once, gives the far end of the keys it can still reach, and the position of the key the
coordinator has reached between the first key it read and that far end says what share of the
range it has read; the entries and bytes read, scaled by what remains, are what is left. The far
end is taken from the range's own keys and not from the end of the table's key space, so keys that
cluster in a narrow band, such as consecutive integer ids, measure as they lie. A share too small
to measure leaves the range as much as has been read, and no range is credited more than a thousand
times what has been read. Until the coordinator has read a few dozen entries the measure says
nothing, and the datastore sizes the parts from its own estimate. A scan that ends within a morsel
of where it stands is never cut, and a scan of many morsels is cut at once, whatever time it has
run.

On `Split` the coordinator asks the transaction to `split` its remainder `[position, hi)` into as
many parts as it fills morsels of `morsel_target_bytes` (default 1 MiB), no more than it has entries
and no more than the datastore answers (1023). When the datastore sizes the parts, it is for parts of
about `morsel_target_bytes`, never less than one entry: the target is raised to the mean entry the
coordinator has read, so a tiny target cuts one entry a morsel and not one part a byte. It keeps
the first part in scan order, narrows its own read to it — the first entry
at or past the cut belongs to the next morsel, which opens its own cursor — and queues the rest in
scan order. Only the part that starts the scan is cut: a morsel queued by a split is not cut again
(§5.3 and §5.4 are how a large one is cut later).

A statement whose reader takes no more than a few rows before it may stop (`State::scan_hint`, the
reads a subquery or `LIMIT 1` declares) does not split before its first flush: the first read reads
no more than the reader asked for, as the serial run does.

### 5.2 Paged bodies split no further than they need

For a body with `START`/`LIMIT` (§8.2) the policy does not split while the rows the paging still
needs (`START` still to skip plus `LIMIT` still to take) are fewer than `morsel_split_rows`: the
coordinator reaches them before morsels would repay their cost. A worker's morsel of a paged body
stops once as many of its entries have reached the paging as the statement could take at all,
since no entry after them can be taken, and the morsels in flight at once are bounded by the
policy (`read_ahead`), so a spent limit has read about a window of the table and not the table.

### 5.3 Stealing: split the largest remainder

A worker that finds its statement's queue empty steals: it asks the running morsel with the largest
estimated remainder to donate. Donation is cooperative and happens at the owner's next chunk
boundary, never while a chunk is running, so no cursor is touched by two threads:

1. The thief sets the owner's steal request (the owner's `Slot`, `morsel/steal.rs`) and waits for
   work, holding its place. The policy decides whether to ask (`Policy::steal`): when every
   morsel donates, always; otherwise when the owner's estimated remainder is as large as two
   morsels (`morsel_target_bytes`), which a cut repays. The wait is bounded by the owner's chunk;
   it ends when the owner answers, when its morsel ends, when the scan stops, or when another
   statement waits for the place (§6.2).
2. At its next chunk boundary the owner reads its resume position `p`, asks `split([p, end),
   Parts(2))` for one boundary `m`, narrows its own end to `m`, and queues `[m, end)` (in a
   backward scan it keeps `[m, p)` and queues `[start, m)`). A worker's morsel narrows its
   cursor (`RangeCursor::narrow`) and the range it parks from; the coordinator's morsel reads
   through the transaction's own cursor, which stops at the first entry at or past the cut, as it
   does for the first split (§5.1). Neither seeks again.
3. With no boundary (a remainder of one storage unit or one key) the owner declines and the thief
   asks the next largest.

A steal takes effect within one chunk of the owner, which is at most one quantum of work. The
donated part is an ordinary queued morsel; its order key is its start in scan direction, which
places it after the owner's narrowed range and before whatever followed the owner, so stealing
never reorders output. Parking (§8.1) already ends a morsel at a chunk boundary and queues its
remainder at its resume key, which is the same operation.

The owner's estimate of what it has left is published at every chunk boundary: the bytes it has
read, scaled by the share of its range still ahead, measured from the first key it read to the
range's far end on the key bytes the datastore splits on. A share too small to measure, which
clustered ids give, leaves the morsel as much as it has read, and no morsel is credited more than
a thousand times that. The estimate chooses whom a thief asks and whether to ask; it never decides
where a range is cut, which `split` does from the first and last keys the remainder holds.

`morsel_steal_always` makes every running morsel donate at every chunk boundary at which `split`
answers a boundary, with chunks as small as `morsel_split_rows`, whether or not a thief asked. It
exists for the width-parity lane: every ordering and failure rule of the merge then holds under
the most donation a run can see, on a backend that gives no view as well.

### 5.4 Re-splitting large morsels

A morsel whose observed size shows its remainder will run far longer than its siblings' (a dense or
hot part of the key space, or a part a single `split` left large because it answers at most
`MAX_SPLIT_PARTS` parts) donates in the same way when the queue runs low, without waiting for a
thief. At a chunk boundary `Policy::resplit` answers a target when no morsel waits, the estimated
remainder is as large as two morsels, and the morsel has read at least two chunks since it was last
cut, which bounds the cuts a wrong estimate asks for. The morsel then splits its remainder into
parts of `morsel_target_bytes` (never less than one entry), keeps the first and queues the rest.
Skew therefore costs a few splits, never balance for the rest of the statement.

### 5.5 TiKV

TiKV is not a priority. It uses the defaults of §4: no region-aligned splitting, no
`surrealdb-tikv-client` change, and no morsels through a view until its client offers one.

## 6. Scheduling

### 6.1 Places and the coordinator

The node keeps one worker pool of **places**, default `available_parallelism − 1`
(`morsel/places.rs`). A **worker** is a tokio task holding a place; it runs morsels of one statement
until the statement has none it may take, then returns the place. A worker takes a place only when
all of the following hold, each answered by the `Policy`:

- the statement holds fewer than `morsel_places` places (default: unbounded; `0` keeps every
  morsel on the coordinator — the operator's escape hatch, and the width-parity lane's
  coordinator-only mode);
- the pool keeps `morsel_oltp_reserve` places free (default 1), so short statements find a core;
- a place is free.

The coordinator always runs morsels too, on the statement's own task and without a place. A
statement therefore makes progress when the node has no free place, and its width is
`1 + places held`. Width is decided automatically; there is no syntax to ask for it.

Where a worker's reads run is a backend property (§4.1): a backend with blocking I/O runs every chunk of a view's cursor on the
affinity pool; a backend with no blocking I/O may run chunks on the worker's own task.

### 6.2 Per-statement queues, node-wide fairness

Each running Range statement owns a **morsel table** (`morsel/set.rs`): its queued, running and
finished morsels ordered by order key. Workers take the queued morsel with the smallest order key,
so the front of the output — what the merge waits for — is worked first, and the coordinator runs
the front morsel itself when no worker has taken it. A worker takes no morsel while as many are
running or finished and waiting for the merge as the policy allows (`read_ahead`, twice the cores),
and returns its place; the coordinator hands morsels to new workers as the merge makes room.

The node's places are served by a dispatcher (`morsel/places.rs`). A statement registers when it
first wants a place and asks for one whenever it could use another: at every merge step of a
Range scan, at every batch it fills. An ask that is refused marks the statement waiting. A free
place is owed to the waiting statement that holds the fewest places, the oldest first among
equals: a statement is given a place only when the free places, above the OLTP reserve, outnumber
the waiting statements owed one before it. The place is not handed over by a callback; the
statement it is owed to takes it at its next ask. With one statement registered the dispatcher
changes nothing: the cap and the reserve are the only limits.

A worker returns its place at the end of every morsel when another statement that holds no more
places than it does is waiting, and otherwise keeps it for the next. It also returns it at a
chunk boundary once it has held it for `morsel_place_quantum` quanta (chunks of its morsel;
default 8) while such a statement waits (`Policy::yield_place`); before it returns the place it
parks, finishing its morsel and queueing the unread rest at its resume key (§8.1), so its work is
not stranded. An idle worker that is waiting for a steal's answer returns its place at once when
another statement waits. Width is then work-conserving — one analytic statement on an idle node
takes every core — and fair: `n` statements with work share the places about evenly.

### 6.3 Fuel and tokio

A worker draws on its thread's fuel meter (`vm::fuel`), capped at the quantum when its task starts,
and yields at the VM's yield point through the host's scheduler: on the native server, back to
tokio. The meter is the thread's, so a worker's quantum bounds that worker's work between yields
whatever the statement, and the coordinator's meter is unaffected by its workers. A chunk asks for
no more entries than the fuel left, so the work between two yields stays bounded whichever thread
ran the chunk.

Tokio is not told about morsel work beyond this: no `spawn_blocking`, no runtime per query. On wasm
there are no workers and every morsel runs on the coordinator, in order.

## 7. Which pipelines run as morsels

The logical plan states what may be split, the physical plan chooses. The logical plan's
`Legality` (`exec/logical/pipelines.rs`) says, per pipeline, whether its source can be cut, whether
its chain reads only its own row and inputs that do not change during the statement, how its sink
combines partial results, how many paging clauses it holds, and the one rule for error order. The
physical plan's one policy (`Policy::plan`, `exec/planner/placement.rs`) decides when the plan is
placed (§7.1), from that legality and the facts the placement pass derives from the plan, and
answers `Decision::Serial` or `Decision::Morsels` with an input; lowering copies the answer into
the program's `Parallel`. The same policy answers every run-time
question — when to split, how big, how many batches in flight, how many places — so no threshold
lives in `morsel/`. When statistics exist (`RFC_STATISTICS_AND_COST.md`) they enter the policy
through its signals and change nothing in the logical layer. The cases the policy exists to make:
fan a selective scan out; keep a scan whose predicate passes nearly every row fused and untracked,
bound by the bandwidth to the client; run a small input as one pipeline.

A pipeline runs as morsels when all of the following hold.

**The logical plan allows it (`Legality::may_split`).**

- **The source can be cut.** `Source::Scan` of a table and `Source::Record` over a record-id range
  are key ranges the KV layer splits. Every other storage source is a sequence the coordinator
  hands on in order: an index range that may reach one record through several keys (such an access
  deduplicates across the whole range, which no morsel sees), a lookup, an index union, a bitmap,
  full-text and KNN candidates, lightweight edges, and a union of them. A value, a statement in
  `FROM`, a sorted or grouped input, a split, a join, a count, a write's targets and a
  once-evaluated input are not partitionable.
- **The chain is row-local.** No expression writes, advances state (sequences), draws entropy,
  blocks (`SLEEP` is never elided, and inside per-row code its pipeline stays serial), runs a
  body the call site cannot see (a stored function, whatever its body does, a closure value, a
  script) or reads outside the transaction's snapshot. Reads of the clock are eligible: per-row clock values carry no
  ordering guarantee, so parallel execution does not change their meaning. Entropy stays
  ineligible until the host supplies an `Entropy` trait whose sources are per-morsel splittable;
  eligibility then follows from what the trait declares, not from the build target.
- **The sink combines.** The output (concatenation, or concatenation through one distinct set), a
  sort (stable merge), a bounded sort (top-k) and aggregates; not a split, a join or a write. Every
  aggregate function is decomposable.
- **At most one paging clause.**
- **The statement allows it (`StatementLegality`).** No expression anywhere in the statement, in a
  nested statement included, writes, advances state, draws entropy, blocks, runs a body the call
  site cannot see or reads outside the snapshot. Such an effect depends on the order or the number
  of evaluations, and the pipelines of a statement share one task's state, so it keeps every
  pipeline of the statement serial, not only the one that holds it. A clock read does not.

**The physical plan allows it (`Policy::plan`).**

- The pipeline writes nothing, nor does any statement around it, and its rows are not put through
  table enforcement. A read nested in a write is placed beside the write and runs serially, unless
  it is a unit loaded from the compile cache (§7.1), which is placed in a scope of its own.
- Every aggregate folds exactly or by appending in order: partial states that combine to the
  serial fold (`Exact`, `Appending`). Float folds (`math::sum`, `math::mean`, `math::stddev`,
  `math::variance`, `vector::sum`) are decomposable too, with the low bits of the result depending
  on the grouping, as a float sum already does across architectures and SIMD paths. The logical
  layer records them as `InexactFloat` and the policy admits them only when the owner accepts that
  the answer differ from `main`'s in its last bits, which the pinned language tests do not yet
  allow; until then they stay serial and admitting them is a change to one line of the policy.
- The body does not rebind its row, read the record a row names or hold a block construct, and
  every statement nested in it only reads and is evaluated afresh for each row. Rows pass through a
  distinct set only when the body hands them to the output with no paging, through one set (§8.6).
- A paging clause is on a pipeline that hands its rows to the statement's output, in a shape the
  merge replays: the body runs the same instructions for a row taken whatever the counts were, its
  `START` skips a row before the row emits anything, its `LIMIT` counts the row straight after, and
  the check that the limit is spent comes last and is the only way to the pipeline's end.

**The input.** The physical plan answers **Range** for a table scan or a record range, a top-k
pushdown's scan among them (§8.4), and **Batches** for every other source that splits. Whether a
body runs inside the storage cursor is not a placement fact: it follows from the code lowering
emits, and narrows the answer when the pipeline runs (§7.2). Whether a Range pipeline's workers
read through a view or the coordinator reads every morsel itself is a run-time fact (§4.1, §5.1);
it changes the width, never the answer.

### 7.1 The decision is a field of the physical plan, and where it is made

The decision is made once, in the physical plan, and stored where the pipeline starts. There is no
physical pipeline node: a pipeline is a cut of the plan's tree, from its source up through the
streaming nodes to the sink its rows end in (`Plan::pipeline_parts`), and the decision is the
`Placement` of the pipeline's source node (a table or record scan, an index, lookup, index union,
bitmap, full-text or KNN access, or a union). A pipeline that has no source node of its own
answers a constant that is `Serial`.

`place` (`exec/vm/place.rs`) runs as the last step of `optimize`, after the rewrite rules have
reached their fixed point. For each pipeline it

1. reads the legality the logical plan stated for the pipeline's source (`Placement.legality`,
   attached when the plan is mapped, and restated by `Legality::fuse` where a rule removes a
   breaker), with the distinct sets the rows pass through fused in;
2. derives the pipeline's `PhysicalFacts` from the plan: the access path, rows put through
   enforcement, writes, the kind of sink, the number of paging clauses and whether the merge can
   replay them, a binding step, a read of the record a row names, distinct sets, nested statements
   and block constructs;
3. asks `Policy::plan` with the statement's refusal, and stores the answer.

It walks every node of the plan, not only those the rewrite rules reach: the members of a union,
the subjects of a write, the statements in `FROM`, the plans nested in expressions, the statements
of a block. It recomputes and overwrites every decision it reaches, since a plan is placed when it
is mapped with the scope of its own statement and placed again with the scope of the statements
around it. The scope is the writes and the refusal of the statement and of every statement that
encloses it, so a read nested in a write, or in a statement that refuses, is placed serial. The
statements of a block are siblings that do not see one another's writes, each placed in the scope
of the plan that holds the block. The pipelines of a `MATCH` plan, the
build and lookup sides of a join and the build of a probe are pipelines that only the physical
plan has, which lowering cuts into plans of their own: the logical plan states nothing for
them, and they are placed serial.

Lowering reads `Placement.decision` and copies it into `Pipeline.parallel`, giving the paging
clause the program slot it emitted. A pipeline the plan did not place is an internal error, and
lowering never decides placement itself. It checks only what can narrow a decision: a combine
stronger than the legality the decision was made against is an internal error; so is a program
that writes holding a morsel decision, checked per scope, program-level pipelines against the
whole program and a group's pipelines against the group; and a decision resting on a physical fact
that the emitted code contradicts becomes `Serial`, which always gives the serial answer. Pipelines lowering creates for itself (host code, view and projection bodies, a once-only
source) are `Serial` by construction.

Run-time units that a program loads from the compile cache (a called function's body, a table's
read) are placed in a scope of their own, as the cache key holds none of the enclosing
statement's facts.

### 7.2 The decision, the emitted body and the mode the VM runs

`in_place` is a fact of the code lowering emits: a pipeline's body runs inside the storage cursor
when its source reads storage and the body writes nothing, rebinds no row, splits nothing, is not
put through enforcement, calls no code that needs the host and holds no backward jump. It is not
an input of the placement policy, and it never changes `Pipeline.parallel`. The physical plan
decides placement; how a placed body executes is a property of the code that was emitted. The VM
combines the two into the mode a pipeline runs in, through one function
(`morsel::effective_input`), and only narrows: it never turns `Serial` into morsels.

| the plan decided | the body runs in the cursor (`Pipeline.in_place`) | the VM runs |
|---|---|---|
| `Morsels`, input `Range` (a table scan or a record range) | yes | Range: workers read their key ranges in place |
| `Morsels`, input `Range` (a table scan or a record range) | no | Batches over decoded rows |
| `Morsels`, input `Batches` (any other source that splits) | no | Batches |
| `Morsels`, input `Batches` (any other source that splits) | yes | serial |
| `Serial` | either | serial |

Making "lowers natively" a property of an expression that the physical plan reads, so that the
mode follows from the decision alone, is later work.

### 7.3 Writes

Writes are not planned as morsels. The owner's decision for a later phase is **parallel compute,
ordered apply**: a write statement's per-row compute (SET/CONTENT/MERGE expressions, function
calls, computed values, the source `WHERE`) runs in parallel across morsels when it only reads,
does not read the table it writes, advances no state and draws entropy only through a splittable
`Entropy`; preparing each write (record bytes, index keys, each morsel's batch of puts) is parallel
too; appending the prepared batches to the statement's transaction in row order, and the side
effects that must be ordered (events, live notifications, change feeds, unique checks, `RETURN`
output), are serial, so observable behaviour matches `main`, including the first failing row's
error. A bounded ordered window charged to the lease carries computed rows to the apply step, and a
bulk write may stage through a host-chosen target with an atomic `commit_staged` (R2 parts on
Cloudflare, the transaction directly on a local engine). That is outside this RFC's phases.

## 8. Combining results

Morsel results are merged in order-key order. Each rule below states how the merged result equals
the serial run's.

### 8.1 Output with no sort: concatenation in scan order

The output is the morsels' output concatenated in order-key order. A key range's morsel hands its
rows to the coordinator as it produces them: the rows of each chunk go into a bounded queue of the
morsel (`morsel/outbox.rs`) and the coordinator moves them to the statement's output while the
morsel runs. Only the output of an unpaged body streams; a morsel over batches of rows hands its
rows over whole, with its batch. A chunk waits in the queue with a grant that charges its bytes to
the run's fan-out lease (`FanoutLease::grant_queued`), reserved when the chunk is queued and
released when the coordinator takes it, when the morsel ends and hands what is left on as its output,
or when the queue drops. The lease refusing a chunk revokes the lease, as any refusal does (§10): the
chunk reaches the coordinator without a grant, the morsel parks after it, and the coordinator runs
what is left of the range in order. A revoked lease refuses every later ask, so each Range morsel
still running queues at most one more chunk without a grant before it parks: what the lease does not
see is bounded by one chunk per morsel in flight.

Where the rows are read in order the coordinator takes them from the **front** morsel — the
unfinished morsel with the smallest order key — and from no other. A morsel counts the bytes it has
queued against `morsel_window_bytes`. A morsel that is not at the front and holds a window parks at
its next chunk boundary: it records its position, drops its cursor back to its worker, and returns
its place. When the front morsel completes, the next morsel's queue is drained to the output and
that morsel becomes the front; a parked front morsel resumes from its position. A morsel holds at
most its window and the chunk that took it past the window.

A pipeline whose rows nothing reads in order merges **freely** (`MergeOrder::Free`): no `ORDER BY`
sorts them, no paging counts them, no distinct set deduplicates them, no operator reads them as its
input, and no `LET` binds them. The coordinator takes rows from any running morsel and absorbs
finished morsels as they finish, so a slow morsel holds back none after it, and the rows reach the
reader in the order the morsels produced them. Its failure is the failure of the first failing
morsel the merge meets, one of the failures of the rows that fail. A `GROUP ALL` whose every
function is `count` merges freely too, since its partial counts add in any order.

The statement's output is consumed through the existing flush (`host::flush`) and the response
stream's backpressure. A slow consumer stalls the queues the coordinator takes from, which fill the
windows, which park the rest: no buffer grows past its window.

### 8.2 `START` and `LIMIT`: the replay, over morsels

A paged body (`Policy::plan`'s paged case) runs each morsel as though every row were taken, except the
rows `START` is sure to skip — those it still skips when every row in flight ahead of the morsel
reaches the paging — and records for each row whether it reached the paging, whether it was
skipped, what it emitted, and how it failed. The merge replays the
paging over the rows in order-key order:

- a row that fails before its paging raises its failure;
- a row `START` skips is dropped, with any failure its projection raised;
- a row taken counts against `LIMIT`; once the limit is spent the statement stops: every
  outstanding morsel is cancelled (§11), the queue is dropped, and the source is read no further,
  so a failure past the limit — of a row, a morsel, or a read — is never raised.

The bounds of §5.2 keep the rows in flight to about what the paging needs, so a small limit reads
about what the serial run reads.

### 8.3 `ORDER BY`: sorted runs and a k-way merge

A sort that keeps every row takes its entries from the morsels as sorted runs. Each morsel sorts
the entries of its rows, stably, when its range is exhausted, on the thread that ran it
(`morsel/sorted.rs`, over `vm/sort.rs`); the entries it hands on are ordered by the sort's keys, ties
in the order the morsel's rows arrived. The statement takes the morsels' entries in order-key order,
each after the entries it holds, so the entries of equal keys lie in the statement's buffer in the
order the serial run reads them. The statement's own sort is the loser tree: it merges what it holds
a quantum of entries at a time (`surrealdb_runtime::fuel::sort_by`), the earlier run winning a tie,
and a quantum that lies inside one morsel's run is already in order. The merged order is the stable
order the serial sort gives, ties by order key and then arrival within the morsel.

A morsel's runs are the runs the bounded-memory spill writes (`exec/memory/runs.rs`): when the lease
refuses a morsel's sort it writes runs to the temporary store as the serial sort does, and hands the
merge a `Partial` of its runs and its sorted buffer. The statement writes what it holds as a run,
appends the morsel's runs after it, and takes the morsel's buffer as its own, so the statement's runs
stay in serial order. The final merge reads memory and spilled runs alike through the loser tree, no
more at once than the query's fan-in (`QueryMemory::fan_in`), in several passes otherwise. Ordering
and spilling are one mechanism.

A sort that keeps every row over a key range merges **by position** (`MergeOrder::Positioned`): the
coordinator takes the sorted run of a morsel as the morsel finishes, and holds it at the morsel's
order key until no morsel lies before it, when the statement absorbs it. The order key is the tie
tag of every entry in the run: a run is stably sorted, so an entry's place among those that tie is
its row index in the morsel, and nothing is stored per entry. Absorbing the runs in order key order
puts equal keys in the statement's buffer in the order the serial run reads them, so the statement's
sort gives the serial stable order whatever order the morsels finished in. The sort lives in the
statement's state and is merged by the statement's own loser tree, so a run is not merged into
another before it is absorbed.

Every result the coordinator holds either holds a place of the morsels in flight or has its whole
size charged to the query's lease, so what it holds is bounded by the policy's read-ahead and the
lease together. A run held in memory takes no place (`takes_a_place`) where it holds entries, the
morsel that built it charged every non-empty sort to the query's lease, which a morsel does only
where the query can spill (`bound_batch`), and the lease also takes the fixed size of holding the
result (`held_result_bytes`): a run is then the sort's content whether the coordinator holds it or
the statement does, so a slow morsel at the front stops none of the morsels behind it. An empty run,
a run of a query with no store to spill to, which is held uncharged, and a run whose fixed size the
lease refuses keep their places, so the results the coordinator holds stay within the policy's
read-ahead. A spilled run and a failure keep their places and are absorbed or raised only when no
morsel lies before them, so the first failure and the order of spilled runs are the serial run's.
A sort over batches of rows merges the batches whole and in order.

### 8.4 Top-k: per-morsel heaps and one shared threshold

`ORDER BY … LIMIT k [START s]` keeps a bounded heap of `k + s` rows per morsel. A morsel whose heap is
full publishes its heap's root to the threshold cell of the run of the program that scans, the same
cell the scan's top-k pushdown reads, and the cell keeps the tightest threshold published
(`TopKThresholdCell::tighten`). Every morsel's scan judges an entry by the cell before the entry runs:
a row strictly worse than the threshold cannot be among the `k + s` best, because `k + s` rows at
least as good exist. A row equal to the threshold is kept, however many keys the order has: the morsel
that published it may lie after the morsel that reads the row, and the row wins a tie with it by
arrival (`TopKThresholdProbe::ties_kept`). A pipeline read as batches keeps the pushdown's own rule:
the batches that publish were dispatched before every row the coordinator has still to read, so a
tie with their threshold loses to them.

The threshold belongs to one run of the program — it is shared by the morsels of one evaluation of the
statement and by nothing else, as the golden `a_top_k_threshold_belongs_to_one_run_of_its_program`
pins for the serial run (each `$parent` evaluation of a subquery has its own). The merge takes the
heaps' rows in the order of §8.3 and applies `START` and `LIMIT`.

A row the threshold rejects is not evaluated, so a Range scan judges entries by it only where its body
cannot raise for any row (the logical `Legality::may_raise`, carried as `MorselPlan::prunes`): the serial scan over
stored bytes evaluates every entry, and a failure it raises on a row the threshold would reject is
raised at every width, in the order of §11. A body holding arithmetic, a cast, a call, a dereference or any
expression not known to answer every row runs every entry. Which rows are rejected, where the body
cannot raise, depends on when the threshold tightens and changes no answer.

### 8.5 Aggregates

An aggregate grouped by nothing folds each morsel's partial state into the statement's in
order-key order (`Groups::absorb`, `GroupState` accumulator merge). Only aggregates whose partial
states combine exactly are planned as morsels (§7), so the result equals the serial fold; an
appending accumulator (`array::group`, `array::collect`) keeps serial order because partials fold
in order-key order; a non-aggregate column keeps the value the earliest morsel evaluated.

`GROUP BY` keys partition the work (`morsel/grouped.rs`). Each morsel builds a partial group map,
charged to the lease. The groups of the morsels that have ended wait until a window of them has
gathered (`Policy::partials_ready`: as many morsels as may be in flight, or two or more holding enough
groups), and a statement that runs rows itself merges what waits before it folds a row. When the
groups that wait repay a division (`Policy::worth_partitioning`) the merge divides the key space into
`P` ranges (`Policy::partitions`, where `P` is the statement's task and the places it can take at
that moment), by splitters drawn from the keys the
morsels hold, and divides each morsel's groups by them, a group moved whole into the range its key
routes to (`Groups::split`). Ranges, not hashes: two keys that compare equal under `Value: Ord` are
one group while their hashes may differ, and a range keeps every key that compares equal to another
in one partition. Each partition folds the morsels' shares in order-key order on a worker or the
statement's task, so first values and appending accumulators keep serial order, and the partitions'
groups, which follow one another in key order, fold into the statement's groups in partition order:
the group-key order `main` hands groups out in. With no place free, or too few groups to repay a
division, the groups fold in order on the statement's task, as a merge of one partition does.

A morsel's group map that the lease refuses spills as the bounded-memory aggregate spill does —
every group's saved state (`Accumulator::save`) in key order — and is read back as a sorted partial,
which merges by key without re-partitioning: the statement writes the groups it holds as a run,
appends the morsel's runs, and the final merge reads the runs and the groups still held in key order,
the saved states of one key folded in the order they were written.

An aggregate whose partial states combine associatively merges **by adjacency**
(`MergeOrder::Adjacent`), over a key range: the coordinator holds the partial states of finished
morsels until no morsel lies before them, and combines those it holds of morsels next to one
another, the earlier before the later, on the coordinator, charging the rows it folds to the
thread's fuel. The fold of a run of adjacent morsels is the fold of the morsels in order however the
run was put together, so appends, first values and joined strings equal the serial run's. A held
result counts with the running morsels against the number the policy lets be in flight, so
combining, which turns several held results into one, is what frees a place. The partitioned merge
of `GROUP BY` partials runs per window of held results, not per adjacent pair. A result that failed,
or that holds spilled groups, is not combined and is absorbed when it reaches the front. A failure
of a held morsel is raised once every morsel before it has merged, so the failure raised is the
failure of the earliest failing morsel. An aggregate over batches of rows merges the batches whole
and in order.

### 8.6 `DISTINCT`

A body whose rows pass through one distinct set, with the output as its sink and no paging, runs as
morsels (`Combine::Distinct`); a body that ends in another sink, passes through two sets or pages the
rows after the set is serial. Each morsel runs the body with a set of its own, so a row it has seen
does nothing, and records for each row it took the row's value, what it emitted and how it failed. The
merge, which takes morsels in order-key order, offers each recorded row to the statement's set: the
first occurrence in serial order is the one kept, its output handed on or its failure raised, and a
row the set has seen is dropped with what it did after the set, which the serial run never evaluates.
A set that outgrows its budget fails the row that outgrows it, before anything the row did after the
set. The statement's set is the one the statement's own rows pass through, so the output equals the
serial run's. The set holds the rows the serial run's holds and is not spilled.

## 9. Snapshot and lifetime

All Range morsels of a statement read through one `Snapshot` (§4.1) taken when the statement first
has a morsel to hand a worker, under the statement's transaction, as of the statement's version.
The coordinator never reads through the view: it reads through the transaction's own cursor. No morsel opens a
transaction, and no morsel reads committed data the coordinator would not. On TiKV the snapshot is
the transaction's start timestamp; on
surrealds, the snapshot version the transaction pinned.

A compiled program never outlives the snapshot it was planned against, and the morsels of a
statement never outlive the statement: the statement returns, fails or is cancelled only after
every worker running one of its morsels has stopped and dropped its cursor and its reader handle
(§11). The transaction's commit or cancel, which waits on outstanding readers, therefore never
waits longer than one chunk.

Nested statements a body runs (an eligible body's read-only subqueries) read through the
transaction. A statement planned as morsels writes nothing (§7), so they read what the view reads.

## 10. Memory

Every byte a morsel run holds is charged to the query's lease (`exec/memory`, `Lease`,
`Reservation`), in chunks, never per row:

- **output windows**: a Range morsel that is not at the front parks once its output holds
  `morsel_window_bytes` (default 1 MiB) and entries remain (§8.1), so no morsel holds more than its
  window beyond one chunk, and no more morsels are in flight than the policy allows; the chunks a
  streaming morsel has queued for the coordinator are reserved from the lease while they wait
  (§8.1);
- **breaker partials**: each morsel's sort entries and group map, through the breaker's own
  `Reservation`, charged as the statement's are and spilling as the statement's do when the query
  can spill (`bound_batch`); the merge takes a morsel's reservations over for the statement's, so a
  morsel waiting to merge still counts against the lease and rows are counted once. A sorted run
  held in memory for the morsels before it is such a reservation, which is why it holds no place of
  the morsels in flight (§8.3);
- **merge state**: the statement's sorts and groups, the loser tree's heads, the groups of the
  morsels that wait for a partitioned `GROUP BY` merge, which keep their reservations until the
  merge takes them over, and the distinct set.

A process past its memory threshold fails at the chunk boundary, as the serial scan does
(`ensure_below_memory_threshold`).

## 11. Errors, cancellation and timeouts

**Failure position.** A failure is located by its morsel's order key and its row's index within
the morsel. The statement raises the failure with the smallest position, and raises it where the
serial run raises it: once every morsel before it has been merged without failing and without
spending a limit (§8.2).

**First failure cancels what cannot matter.** When morsel `m` fails, every morsel after `m` in
order-key order is cancelled at once: whatever they produce is either after an earlier failure or
after `m`'s. Morsels before `m` keep running, since one of them may fail earlier or spend a limit
first. A read failure of the source is a failure at the position the read stood at.

**Cancellation** is a flag per morsel and the statement's `Cancellation`. A worker checks both at
every chunk boundary and every yield point, so a cancelled morsel stops within one chunk. A
cancelled morsel's partials and window are dropped and its reservations released. The coordinator
awaits every worker of the statement before it returns, whatever it returns; a worker task is
also aborted when its handle is dropped, but abort cannot interrupt a chunk in progress, so the
await is what guarantees readers are dropped before the transaction finishes (§9).

**Timeouts.** The statement's deadline lives in its `Cancellation` and is checked at every yield
point of every worker, as the VM checks it for a serial run. Once the deadline has passed, the
statement fails with the timeout error unless an earlier-positioned failure has already been
merged; the timeout is raised once, by the coordinator, and failures workers raise after it are
dropped.

**Client disconnect and `KILL`** cancel the statement's context; workers observe it as above.

## 12. Determinism

| behaviour | serial run on `main` | morsel run |
|---|---|---|
| Rows of a scan with no `ORDER BY` | key order, in scan direction | the same: concatenation by order key (§8.1) |
| `ORDER BY` ties | arrival order | the same: ties broken by order key, then arrival (§8.3) |
| `GROUP BY` output | group-key order | the same (§8.5) |
| First value of a non-aggregate column | earliest row | the same: earliest morsel (§8.5) |
| `array::group`, `array::collect` | arrival order | the same: partials fold in order-key order |
| Float folds (`math::sum`, `math::mean`, …) | left-to-right float addition | decomposable, serial for now: a regrouped float sum differs in its last bits (§7) |
| Clock reads per row | handed out in key order | eligible: each row reads the clock when it runs, no ordering is promised |
| Randomness, generated ids, sequences per row | handed out in key order | serial: pending the host `Entropy` trait; sequences never |
| Which failure is raised | the first row in key order that fails | the same (§11) |
| Failures past a spent `LIMIT` | never raised | never raised (§8.2) |
| Rows read past a spent `LIMIT` | none beyond the read in progress | at most what the morsels in flight read (§5.2) |
| Response stream items | per statement | the same: the coordinator emits them |

A morsel run may evaluate rows the serial run would not reach (rows after a failure or past a
limit, in morsels that ran ahead). The body is side-effect free and the outcome of such rows is
discarded, so the extra work is unobservable except as time.

## 13. Observability

`EXPLAIN` and `EXPLAIN ANALYZE` render as they did before morsels, at every width. The operator
counters `EXPLAIN ANALYZE` renders (`rows`, `batches`) include every row any morsel read or
produced, whether the coordinator's, a worker's, or one the coordinator ran through the
transaction cursor, so the text does not depend on how the scan was cut. Eligibility is a property
of the program and width is not, so neither is shown.

Under `morsel_explain_analyze` (default off, so the text above holds by default) `EXPLAIN
ANALYZE` renders what the statement's Range scans did, once beneath the plan, since every operator
of a program reports one set of metrics: a `Morsels:` block in text and a `morsels` object in JSON,
both withheld where the volatile attributes are redacted. It counts the scans that began as one
morsel (`initial`), the morsels cut off by the first split, donated by a steal, cut off by a
re-split, parked and resumed, and cancelled; the steals asked and declined and the places yielded;
the places held at most and on average; the rows and time of the coordinator's morsels and of
workers'; per-morsel rows, bytes, chunks and elapsed time (min / p50 / p95 / max); the time the
coordinator waited for the front morsel; the most output bytes a morsel held; and a listing of the
first 16 morsels by **ordinal only** — the position in which the merge took them, never key
positions, which carry record ids. A pipeline over Batches input, and a statement analysed inside
a block, add nothing to it. The sorted runs, the rows the top-k threshold rejected and the
partitions of a group merge are counted in the test hooks and rendered nowhere.

The counters are kept for every scan: integers touched when a morsel is queued, claimed, parked or
merged, never per entry. They go to the test hooks (`morsel/test_hooks.rs`) per datastore, and a
trace-level span per merged morsel carries its ordinal and outcome, never key bytes. Node metrics
for places in use, queued morsels and steals are not exported.

## 14. The same unit on a leaf

A morsel is the work item a coordinator ships to an edge leaf running on surrealds. Nothing in
§3–§12 assumes the worker shares the coordinator's process; a leaf is a worker with a slower link.

**The work item** is: the block (the encoded pipeline and its units, by identity), the morsel's
range and direction, the snapshot version, a partition id (statement, pipeline, order key), the
ISA range and feature flags the block needs, and the statement's top-k threshold when there is one.

**Leaf eligibility** adds to §7: the program is leaf-clean (`leaf::profile`, `VM_LEAF.md`), and the
shipper encodes it (the encoder refuses, among others, programs serving a late-bound statement).
On Cloudflare leaves the clock is frozen for the request: a platform limitation of leaf execution,
not a reason to make clock-reading bodies ineligible.

**Acceptance and fallback** follow the shipped-program contract's rolling-release rules: a leaf
advertises the ISA range and features it accepts; the coordinator places a morsel on a leaf only
when the leaf accepts the block's ISA and features, and otherwise runs the morsel itself. No
common ISA, a manifest the leaf's bundle cannot serve, or a leaf that fails or times out — each
makes the coordinator run the morsel, or its remainder from the last position the leaf reported,
locally. Shipping makes a query slower during a rollout or an outage; it never makes it fail.

**The result stream** a leaf returns is the morsel result of §3 as frames: output pages in the
rule 7 batch format, sorted runs and partial groups in an encodable partial-state form, position
reports (so the coordinator can split or retry the remainder), and a complete or failed marker.
A morsel is re-run only from a reported position over the same snapshot, and an eligible body's
answer does not depend on where it ran (§7), so a re-run yields the rows a failed leaf did not
report, and no others.

**Placement** is the scheduler's (§6) with one more decision: where a morsel runs. On a single node
every morsel is local. A leaf may split the morsel it received among its own places; the
coordinator sees one result stream.

## 15. Relation to existing code

- **One parallelism unit.** Morsels (`exec/vm/morsel/`) are the only unit of intra-query
  parallelism in the VM; the logical `Legality` and the physical `Policy::plan` decide what runs as
  them. Index, lookup, index-union, bitmap, full-text, KNN and lightweight-edge sources run as
  Batches morsels, not range morsels, until a later phase reads them in parts.
- **Counts.** A count of a range is a morsel run whose body counts. `Source::Count` over a record
  range runs through the morsel scheduler with `count_bounded` as the morsel's read, and the
  backends' internal sharded `count` becomes the backend's `count_bounded` over a view;
  `split_range` (now in `kvs`) serves both. One splitter, one pool, one place to tune.
- **The read frontier** (`RFC_LOGICAL_CONTROL_FLOW.md` §5.5) names the ranges a request reads.
  The first split of a morsel pipeline can use the frontier's `estimate`s, issued when the
  transaction opens, instead of waiting for the threshold of §5.1.
- **Pipelines and parallel legality.** `RFC_LOGICAL_CONTROL_FLOW.md` (Pipelines and parallel
  legality) states the logical side: what may be split, overlapped or fused, and where.

## 16. Implementation plan

Each phase is a sequence of small PRs, each with its tests and, where it changes speed, a
benchmark. Phases 1 to 3 are implemented. Phase 1 builds on the VM pre-emption components (the fuel
meter workers run under, the host scheduler, `count_bounded` and `split_range`); it needs neither
the bounded-memory components nor the shipped-program format.

**Test lanes used throughout:**

- **Width parity.** The language suite runs with the default configuration, with
  `morsel_split_rows = 1` and `morsel_target_bytes = 1` (the "tiny morsels" lane of `EDGE_PLAN.md`
  item 10), and with `morsel_places = 0` (coordinator only), on kv-mem and on SurrealKV, and every
  answer must equal the default run's. From phase 3 the tiny lane also forces a steal at every
  chunk boundary (`morsel_steal_always`). `morsels_answer_what_the_serial_run_answers` and the
  tests of `exec::vm::tests_morsel` assert it for the VM.
- **Read view.** A KV conformance test in `kvs-test` per backend that gives a view: morsels of a
  range tile it exactly; concurrent commits during a morsel run are invisible; commit and cancel
  wait for outstanding cursors.
- **Benchmarks.** On SurrealKV with a 10M-row table: `SELECT count() FROM t WHERE <selective>`;
  `SELECT math::max(n), count() FROM t GROUP ALL`; a selective filter with a projection streamed
  to the client; `ORDER BY n LIMIT 100`; `GROUP BY` a low- and a high-cardinality key; and two
  analytic statements concurrent with a point-read workload, reporting the point reads' p99. No
  phase promises a speed-up; each reports the numbers.

**Phase 1 — in-place scans, no paging. Implemented.**

1. `kvs`: the `Snapshot` and `RangeCursor` traits with `None` defaults; the first backend implementation;
   conformance tests.
2. `kvs`: `estimate` and `split` with the two-seek default; `split_range` in the `kvs` crate with
   its tests.
3. `vm/morsel/`: the morsel table, the coordinator-first split (§5.1), workers on the shared pool,
   Range input for a table scan or a record range with an in-place body, Batches input for every
   other pipeline that may run in parallel, aggregates that fold exactly, output concatenation
   with windows and parking; failure ordering and cancellation; the width-parity lane.

**Phase 2 — `START`, `LIMIT` and early termination. Implemented.** The paged replay over morsels
(§8.2), the bounds of §5.2, `scan_hint` honoured; tests for failures before and past the limit, and
for rows read past a limit.

**Phase 3 — Stealing and fairness. Implemented.** Cooperative steal and re-split (§5.3–5.4), the
node-wide place dispatcher and `morsel_place_quantum` (§6.2), `EXPLAIN ANALYZE` morsel metrics
(§13). Tests: donation never reorders output at either direction, a morsel with no boundary
declines, a hot morsel re-splits, the dispatcher serves the statement holding fewest, a place is
yielded after its quantum, the analysis text is the same at every width by default. Benchmarks
still to run: a skewed table (ids clustered in one band); the concurrent point-read p99.

**Phase 4 — `ORDER BY` and top-k. Implemented.** Per-morsel sorted runs merged by the statement's
loser tree (§8.3), spilled runs merged as in-memory ones, the shared threshold (§8.4) read by the Range
scans whose bodies cannot raise. Tests: sorted runs merge to the serial order, with many equal keys, at every width;
spilled morsel runs merge as in-memory ones; a top-k prunes against one shared threshold and answers
the serial run's rows.

**Phase 5 — `GROUP BY` and `DISTINCT`. Implemented.** Partitioned group merge (§8.5) with spilled
partials; merge-side distinct (§8.6). Tests: low- and high-cardinality groups answer in group-key
order, spilled partials merge by key, a float fold stays serial, a distinct set keeps first
occurrences and fails past its budget at the serial row.

**Phase 6 — More sources.** Non-repeating `Source::Index` ranges and unions of ranges as Range
morsels; `Source::Count` over morsels and the backends' sharded counts folded in (§15).

**Phase 7 — Other backends.** kv-mem: the owned read view. SurrealKV and kv-mem: the transaction's
pending writes frozen at statement start, so read-only statements inside a writable transaction
parallelise and see their own earlier writes.

**Phase 8 — Leaves.** The encodable work item and partial state, the leaf result stream, placement
with ISA negotiation and coordinator fallback (§14), on top of the shipped-program format and its
rolling-release rules.

**Later — writes.** Parallel compute with ordered apply, and staged commit (§7).

## 17. Decisions

Each decision below is stated in the sections above, as built or as planned.

1. **Clock and entropy.** Reads of the clock are eligible in every morsel body; on Cloudflare leaves
   the clock is frozen for the request, a platform limitation documented as such. Clock and
   entropy come from runtime traits the host supplies (like `TempStore` and `MemoryGovernor`): a
   `Clock` that declares whether it is per call (native) or per request (Cloudflare) and an
   `Entropy` source (the OS generator natively, `crypto.getRandomValues` on Cloudflare); eligibility
   follows what the trait declares, never the build target, and tests plug in a fixed clock and
   seeded entropy, which makes the width-parity lanes deterministic. Entropy in a morsel body waits
   for the trait to declare per-morsel splitting; until then it is serial. Sequences
   (`ADVANCES_STATE`) are never eligible.
2. **Float folds** are decomposable and eligible in principle; results may differ from the serial
   run in the low bits (summation order), as they already can across architectures. The policy
   keeps them serial until the pinned results allow the difference (§7).
3. **Where workers run is a backend property.** A backend with blocking I/O: the affinity pool. A backend with no blocking I/O (kv-mem) may run chunks on tokio tasks.
4. **Default width** is `available_parallelism − 1` places, shared node-wide, with a per-query cap
   (`morsel_places`) and an OLTP reserve (`morsel_oltp_reserve`).
5. **Fully automatic.** `morsel_places = 0` is the operator's escape hatch, not the way parallelism
   is enabled. There is no syntax.
6. **Through the transaction, via a backend capability**: an owned read view (a snapshot plus the
   transaction's pending writes frozen at statement start, `Send + Sync`, lock-free to hand to
   workers). Read-only statements inside a writable transaction then parallelise and still see
   their own earlier writes; statements that read and write stay serial. The read-only view
   first; kv-mem next; writable transactions after. Writes use parallel
   compute with ordered apply, and bulk writes stage through a host-chosen target (§7); both are
   outside the phases above.
7. **TiKV is not a priority**: no region-aligned splitting, no client fork.
8. **Cooperative stealing** at chunk boundaries (§5.3).
9. **The KV interface is decided** (§4): `snapshot()`, a re-aimable `RangeCursor`, `estimate`, a
   record-aligned `split`, an opaque resume position, pages in the rule-7 batch format; generic
   trait methods with defaults, so every backend is correct from day one. SurrealKV overrides
   natively first; kv-mem adds an owned view and cursor re-seek; surrealds implements it natively with page-file-aligned splits. A staged write set
   (`stage` / `commit_staged`) joins the contract with the write phase.
10. **`EXPLAIN ANALYZE` shows ordinals only**, never key positions (§13).
11. **One parallelism unit.** Morsels are the only one. KNN, full-text and bitmap sources run as
    morsels of the Batches input today and become range-read morsel sources in a later phase.
12. **`SLEEP` is never elided.** A statement-level `SLEEP` is a coordinator sequencing point
    between statements, and `SLEEP` inside per-row code keeps that pipeline serial.
