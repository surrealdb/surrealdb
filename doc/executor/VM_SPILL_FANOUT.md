# Spilled runs as the currency of spill and fan-out

**Status:** design for three stacked changes on the bounded-memory base
(`TempStore`, `Run`/`RunSink`/`RunSource`, `Runs`/`Merge`, `QueryMemory`,
the spilling sort, keyed groups, GQL distinct and GQL/SQL joins, and region
batch parallelism). Nothing below is built until its step lands.

| Step | Branch | Builds on | Delivers |
| ---- | ------ | --------- | -------- |
| A | `vm/spill-runs` | bounded-memory | run header and trailer, store-global run names, LIMIT/top-k in merge and reduce |
| B | `vm/spill-partials` | A | one `Partial` per breaker for a spilling region batch and for a fan; one coordinator merge over partials |
| C | `vm/spill-partitions` | B | range partitioning of keyed breakers into N partitions, proven equivalent to the unpartitioned finish |

## The thesis

A sorted spilled run and a fan's partial result are the same object: a
sequence of records `(keys, row)` in a stated order, written once through a
`TempStore` and read back once or merged. Local spill writes runs because a
breaker outgrew its lease; a region batch or a fan writes runs because its
result must travel to a merge that is not on its own stack. Either way the
consumer is the same k-way merge (`Runs::merge` over a loser tree) with the
same tie rule — the earlier source wins — so a merge over partials answers the
order a single serial run answers.

Four rules fix the boundaries of this design:

1. **No recompilation, no replanning.** A program is compiled once, before it
   runs. Every decision below is taken by the VM at a buffer push, against the
   lease, or by the physical plan as a static label.
2. **Spill is VM-intrinsic.** A breaker spills when its `Reservation` says
   `Verdict::Spill`. The plan only labels which breakers may spill and how their
   partials merge.
3. **No filesystem is assumed.** Everything is expressed against `TempStore`,
   whose implementations may be files, memory, a Durable Object, or an object
   store.
4. **Main's results do not change.** Every merge preserves main's order,
   stability, first-value, and error semantics exactly; where an exact
   equivalence cannot be proven the design falls back to the unpartitioned path.

Temporary runs are not durable state: they never outlive the process that
wrote them (or the session sweep), and no released binary reads a run another
release wrote. The upgrade suite (`language-tests/tests/upgrade/`) does not
apply; the run header's build key enforces the same property at run time.

## Facts the design relies on

### Key equality is `Value: Ord`

Groups (`groups.rs` `KeyIndex`), the GQL distinct (`SeenSet`, a
`BTreeSet<Value>`), and the join build table (`BuildTable`, a
`BTreeMap<JoinKey, _>`) all decide key equality by `Value::cmp`. The spilled
finishes order their runs by `a.cmp(b)` on the key vector for the same
reason. No breaker that decides equality uses a hash.

`Number`'s order compares across variants:

- `Int` against `Float` is exact (integer parts as `i128`, then the float's
  fraction), so `1 == 1.0f`.
- `Int` against `Decimal` is exact (`Decimal::from(i64)`), so `1 == 1dec`.
- `Float` against `Decimal` compares integer parts, then fractional digits by
  repeated scaling in `f64`, and is **approximate**: `0.1f == 0.1dec` holds
  although the decimal nearest `0.1f` differs from `0.1dec`, and the relation is
  not transitive across the boundary (`Number`'s `Hash` documentation records
  `0.11111f != 0.11111dec`).
- `Number: Hash` hashes the decimal expansion: `Int(1)`, `Float(1.0)` and
  `Decimal(1)` agree, `Float(0.1)` and `Decimal(0.1)` do not.
  `Value::hash_agrees_with_eq` reports `false` for any value that holds a number
  anywhere.

`KeyOrder { ascending, collate, numeric }` belongs to sort keys alone.
`collate` and `numeric` change only the comparison of two top-level strings
(`lexical_cmp`, `natural_cmp`, `natural_lexical_cmp`), and every other pair falls
back to `Value::partial_cmp`. Grouping, distinct and join equality never read a
`KeyOrder`. A sort's ties under `collate`/`numeric` are decided by those
comparisons, and its stability by the arrival stamp `compare_rows` reads after
the keys.

### What exists to build on

- `TempStore`: `part_size`, `quota`, async `write_run`/`read_run` returning
  `BoxFut`, sync `delete_run`. Every part but the last is **at least**
  `part_size` bytes. `RunId` is a `u64` local to the store.
- Run encoding (`runs.rs`): a header, checksummed blocks of records and a
  trailer, laid out below; a record is a `u64` BE key count, then each key and
  the row as a `u64` BE length followed by the value's revisioned encoding. A
  reader holds one part, plus the block and block header that cross into it
  (`buffer_bound`).
- `QueryMemory::headroom = buffer_bound(part_size) * (1 + fan_in)`, where
  `buffer_bound(part_size) = part_size + min(64 KiB, part_size) + 8` is what a
  run's writer or reader holds (a part, a block and its header), `fan_in =
  spill_merge_fan_in` (default 8), `part_size = spill_part_size` (default 1 MiB).
- `Runs::reduce` merges consecutive runs a fan-in at a time until at most
  `readers` remain; `Runs::merge` reads them with the held records as the last
  source; `Merge` breaks ties by source index.
- A sort with `keep` (a limit the plan pushed into it) prunes to the rows
  `keep` can reach. Where the query has a temporary store it takes a lease
  account (`bound_breakers`); with none it takes no account and never fails for
  memory.
- Region bodies admit sorts and aggregates only (`body_admits` refuses
  `JoinProbe`, `Distinct`, `SplitPush`, writes). A batch runs on a `State` of its
  own with no lease account; its result is absorbed in batch order
  (`Region::absorb`, `State::keep_absorbed`, `Groups::absorb`).
- `BuildId { ISA_VERSION, CARGO_PKG_VERSION, capabilities }` names the build that
  compiled a unit (`compile_cache.rs`).

## Step A — the run as a self-describing, shareable object

### A.1 Run header and trailer

A run gains a header before its first record and a trailer after its last.
Their purpose is that a reader **rejects** a run it cannot interpret, with an
error, rather than decoding records under the wrong assumptions. They do not
describe how to interpret records: fans and coordinator run the same compiled
program, so accumulator descriptors, field layouts and function identities are
not serialised. Anything that could differ between writer and reader is
covered by the build key and the program key instead.

Header, fixed layout, big-endian:

| Field | Size | Contract |
| ----- | ---- | -------- |
| magic | 8 | `b"SRLSPILL"`; anything else is not a run |
| format | 2 | run format version; a reader accepts exactly the versions it was built to read (today: one) |
| build key | 16 | FNV-1a-128 over `ISA_VERSION` (u32 big-endian), then the format (u16 big-endian), then the bytes of `CARGO_PKG_VERSION`, fixed-width fields first so the input is unambiguous; a reader rejects a run whose build key differs from its own |
| program key | 16 | the identity the dispatcher gave this query's program (step B); zero for a run no other process reads |
| breaker kind | 1 | `Sort`, `Groups`, `DistinctSeen`, `DistinctDeferred`, `DistinctKept`, `JoinBuild`, `JoinProbe`, `JoinOut` |
| breaker index | 2 | the breaker's index in the program (`sorts[i]`, `aggregates[i]`); a SQL join writes its `JoinSpec::build` group, a GQL join its `JoinStep::build`, and a GQL distinct `0`, which has no index of its own |
| key arity | 2 | the key count every record carries |
| order | 1, then 2 + n for a key list | tag `0` for plain `Value::cmp` on the key vector; tag `1` for a `KeyOrder` list, followed by a `u16` count `n` and `n` bytes, one per key (`ascending`, `collate`, `numeric` bits). A stamped sort carries one more key than its list; keys past `n` compare by `Value::cmp` |
| block size | 4 | the largest block payload the writer used: `min(64 KiB, part_size)` |

The build key is computed in const context with an in-crate 128-bit FNV-1a
(no dependency). It deliberately excludes the capabilities part of `BuildId`:
a run's bytes do not depend on capabilities, and the program key already
distinguishes programs.

Records are written in **blocks**. A block is `u32 len, u32 crc32c, payload`,
where the payload is whole records, at most `block size` bytes, except that a
single record larger than the block size is a block of its own. A writer
writes a part only after sealing a block, so it holds up to a part and one
block, and a reader holds a part and the partial block that crosses into the
next. The block size is therefore the overshoot of each source and of the
writer past `part_size`, which `QueryMemory::headroom` reserves through
`buffer_bound` (a part, a block and a block header per buffer); the record
being encoded or decoded comes on top. The reader
checks each block's CRC before decoding any record in it, so a damaged or
mis-assembled run fails before a single row of the damaged block is handed on.
CRC-32C is a table-driven implementation in `memory/runs.rs` (about thirty
lines, no dependency). The block size is `min(64 KiB, part_size)`.

Trailer: `u32 0` (an empty block length marks the end), then `u64 record
count`, `u64 block count`, `u64 payload bytes`, and `b"SRLSPEND"`. A reader
that reaches the end of the store's bytes without reading a trailer, or whose
counts disagree with the trailer, fails with "a spilled run is truncated".
Bytes after a well-formed trailer fail as a malformed run.

The header and trailer are written by `RunSink` and checked by `RunSource`;
`TempStore` sees opaque bytes. `RunSink::open` takes a `RunSpec { kind, index,
arity, order, program }` and `Run` keeps it, so `Runs::merge` asserts that
every run it merges has the same spec (a merge of runs of two breakers is a
bug, raised as an internal error).

Rejected alternatives: a per-part checksum keyed to the store's parts
(part boundaries are the store's business, and a re-chunking store moves them);
a whole-run checksum in the trailer only (detects damage after its rows were
handed on).

### A.2 Store-global run names

```rust
pub struct RunRef {
    /// The store-wide prefix of the session that wrote the run: node, then session.
    pub session: SessionName,
    /// The run's id within its session.
    pub id: RunId,
    /// Which tier holds it, and therefore who can open it.
    pub tier: Tier,
}

pub enum Tier {
    /// A store only this process can read: files, memory, a Durable Object's storage.
    Local,
    /// A store every node configured with the same backend can read.
    Shared,
}
```

`TempStore` changes:

- `write_run(&self, sharing: Sharing) -> BoxFut<Box<dyn RunWrite>>`, where
  `Sharing::{Private, Shared}`. `RunWrite::finish` answers a `RunRef`.
- `read_run(&self, run: &RunRef)` and `delete_run(&self, run: &RunRef)`.
- `fn session(&self) -> &SessionName` and `fn shares(&self) -> bool`.

Contracts:

- A store whose `shares()` is false answers every `RunRef` with `Tier::Local`
  and refuses `Sharing::Shared` with an error. `FileTempStore` and
  `MemTempStore` are local. `MemTempStore::shared(...)` is a test constructor that
  makes two store handles over one map, so cross-node opening is testable.
- `TieredTempStore` writes `Sharing::Private` runs as it does today (local tier
  first, overflow past capacity) and answers `Tier::Local` for them, even when
  some of their segments lie in the overflow: the run as a whole is only
  readable by this process, since its segment map is in memory. A
  `Sharing::Shared` run bypasses the local tier and is written to the overflow
  alone, and its `RunRef` is the overflow's own, `Tier::Shared`.
- A `RunRef` is a name, not a capability: opening a run of another session
  requires the reader to hold a handle on the same backend and passes the
  header's build key and program key checks.
- Ownership: the session that writes a run deletes it, unless it hands the
  `RunRef` on (step B's fan result). From the hand-off the receiver owns it:
  its `Run` deletes it on drop through the receiver's store handle. The writer's
  sweep never removes a handed-off run while the writer's session lives; once
  the writer's session is dead, its runs are swept regardless (a coordinator
  whose fan died re-runs the fan or fails the query; it never reads a swept
  run).

### A.4 LIMIT and top-k in merge and reduce

`Runs::merge(memory, held, order, limit: Option<usize>, ctx)` and
`Runs::reduce(memory, order, readers, limit, ctx)`:

- every intermediate pass of `reduce` writes at most `limit` records: once the
  pass's merge has yielded `limit` records the pass stops reading and finishes
  its run;
- the final `Merge` yields at most `limit` records and then answers `None`
  without reading further;
- `Runs::spill_sorted` with a limit writes at most `limit` rows of the sorted
  buffer.

Correctness: the merge is stable and a run's first `limit` records are the
only ones of that run that can be among the merged first `limit` (any record
after them has `limit` records at or before it from that run alone, each
winning a tie against it by source index or arrival). So truncating every run
and every pass to `limit` answers the same first `limit` records in the same
order.

Which sorts get a limit:

- A sort with `keep` of a query that has a temporary store is charged to its
  lease like any other sort once `keep` exceeds what one chunk of the lease
  covers (`keep * estimated row bytes > chunk`); while the lease covers it, it
  prunes in memory. With no temporary store it takes no account and prunes in
  memory whatever it holds.
  When it spills, it sorts its buffer stably and truncates it to `keep`. Runs
  are written in arrival order and the merge breaks ties by source index, so
  ties keep their arrival order across runs whether or not the rows carry the
  arrival stamp `keep_sorted` adds. Its merge carries `limit = keep`. A random `keep` sort spills with fresh random
  keys per row; the first `keep` rows by independent uniform keys are a uniform
  sample in uniform order, which is what `shuffled` answers.
- A sort without `keep` merges with `limit = None`. The final merge already
  stops when the pipeline reports `stopped` (`run_rows`), and no pass is made
  for a merge that no longer needs one.

A sort of a brute-force KNN (`SortSpec::knn_slot` is set) never takes an
account and never spills, because its finish publishes distances that the
spilled finish does not.

A spilled `keep` sort records a **spilled floor**: the keys, without the
arrival stamp, of the last record of a run that holds exactly `keep` records,
the least such. `Instr::SortPush` drops a row that does not order strictly
before it, as it does for the in-memory floor. The floor is exact: the `keep`
rows of that run all arrived earlier, each orders at or before the run's last
record, and each wins every tie against a later row by source order, so a later
row that does not order strictly before the floor cannot be among the first
`keep`. The floor is reset whenever the sort is reset.

`keep` sorts that never exceed a chunk, and those of a query with no temporary
store, take nothing from the lease, so the in-memory top-k path takes no lease
and never spills or fails for memory.

## Step B — one partial type, one coordinator merge

### B.1 `Partial`

```rust
/// What one producer — a region batch that spilled, or a fan — hands its
/// coordinator for one breaker.
pub(crate) struct Partial {
    /// The breaker and order every run of this partial was written under;
    /// `held` records have the same arity, `side` keys are the join width only.
    pub spec: RunSpec,
    /// The producer's runs, in the order it wrote them.
    pub runs: Runs,
    /// Records the producer still held, after every run, in the spec's order.
    pub held: Vec<Record>,
    /// Records that travel beside the runs out of key order: a join build's
    /// rows whose key holds a regex, `(key, row)` in arrival order. Empty for
    /// every other breaker.
    pub side: Vec<Record>,
    pub counts: PartialCounts,
}

pub(crate) struct PartialCounts {
    /// For a join build, the build rows the producer read, regex rows
    /// included; for a distinct, the rows it took; otherwise the records the
    /// partial holds.
    pub taken: u64,
}
```

`Partial` lives in `exec/vm/partial.rs`. Region's batch struct is
`BatchResult`, so the two never share a name.

A producer emits one `Partial` per breaker it ran. A partial's runs are written
with the producer's program key. A fan that hands its partial to another
process writes its runs `Sharing::Shared` and seals its held records first
(`Partial::seal`), so the partial travels as names and counts alone.

The records of a partial are those a single node writes for the same breaker,
with these differences, which let the order of a whole record vector stand for
serial order across producers:

- **Sort runs carry the sort's keys alone.** A sort's records are cut to the
  sort's keys after they are stably sorted and before they are written, and a
  sort run's arity is the number of keys (one for a random order), never the
  width of the first row. An arrival stamp is local to the state that wrote it,
  and whether a sort stamps at all depends on the thread's quantum, so stamps
  are not compared across states. `sort_order` compares keys only; records that
  tie keep the order of their sources, which is exact because each run is
  written in arrival order, stably sorted, and a merge never reorders a source.
- **A distinct partial's records are `[row, producer, arrival]`**, and a join
  build partial's are `[key.., producer, arrival]`. A single node's spill keeps
  its own arity, and a partial's runs never share a `Runs` with a single node's.
- A distinct producer keeps only the first arrival of each row it holds.

### B.2 The coordinator merge

`merge_partials(memory, how, partials, ctx)` merges producers in **producer
order** — the order the source yielded the batches, or the order of the fans'
key ranges — which is the arrival order of the serial run. Within one producer,
its runs come first and its held records last. Every partial must be of one
breaker and agree in kind and arity with `how`; otherwise the merge fails with
an internal error. `how` is a `PartialMerge`: `SortMerge { keys, keep }`,
`KeyedFold`, `DistinctFirst { max_rows }` or `JoinBuild { max_rows }`.

The merge **seals** rather than merging an ordered list of sources: every
producer's held records but the last producer's are written as a run after that
producer's runs. What is merged is then a list of runs in producer order and the
last producer's held records, which arrived after every run: the single-producer
case, `Runs::merge`, whose earlier-source-wins tie rule gives every tie the
serial order. Sealing keeps passes of `Runs::reduce`, which merge consecutive
runs, correct without a segment boundary.

Per breaker:

- **Sort** — merge under `sort_order(keys)` with `limit = keep`. Each producer's
  held records are cut to `keep` first, which is exact for the reason a merge
  with a limit is.
- **Keyed aggregate** — merge by key (`Value::cmp` on the key vector); the
  caller folds the result with `SavedFold`: for each key, every record's saved
  states are loaded with `Groups::load_saved` in source order, the first opening
  the group and each later one merged after it, and a first value kept from the
  earliest source that has one. `GROUP ALL`'s one group is saved under an empty
  key (`Groups::into_saved`), so fans fold it as any other.
- **GQL distinct** — `merge_distinct` reduces the runs to one reader fewer than
  the fan-in, keeps the first row of each equal run, and puts the kept rows back
  in `[producer, arrival]` order by a merge of runs of kept rows. The distinct-row
  cap is checked against the rows kept, so the error and its threshold are
  main's.
- **Join** — `SpilledJoin::from_partials` appends the producers' build runs in
  producer order, writes each producer's held records as a run, and concatenates
  the regex rows (`side`) in producer order, which is serial arrival order. The
  build row cap is checked against the sum of the producers' `taken` counts
  before any probe row is deferred. `SpilledJoin::finish` slices the key columns
  off each record, so the producer and arrival columns only order the build.

### B.3 Region batches that spill

Region bodies hold only sorts and aggregates, so a region batch produces sort
and group partials only.

- A batch whose body is not paged binds its breakers to the query's lease when
  the query can spill (`bound_batch`): one `Reservation` per spillable breaker,
  charged at the buffer pushes the statement's state is charged at. A sort in a
  batch never spills by `TEMPFILES` row count. After every row the batch spills
  what its lease no longer covers, as the statement does. A paged batch is never
  bound.
- While no batch account refuses, the batch's buffers are absorbed
  (`keep_absorbed`, `Groups::absorb`) and the absorb path does not touch the
  store. The `vm_alloc` measurements hold this path to its allocations.
- A batch that spilled a breaker hands the merge a `Partial` for it in place of
  its buffer. A spilled breaker's reservation is taken out of the batch's state
  before its buffer becomes a partial, so the lease keeps counting the buffer
  until the partial's records are charged to the same reservation. The batch's
  reservations travel with its result, and the merge takes each one over for
  the statement's account for the same breaker (`Reservation::adopt`) before
  it charges the statement for the rows it takes. A batch waiting to merge
  therefore counts against the lease, the batches that start after it find the
  lease short and spill sooner, and the rows a merge moves are counted once,
  not by the batch and the statement together.
- To absorb a `Partial`, the merge first **seals** the statement's own state for
  that breaker (`seal_sort`, `seal_groups`): it writes what the statement holds
  as a run, in memory order, which is serial order, so that every record the
  statement holds precedes the batch's. It then appends the batch's runs after
  the statement's and takes the batch's held records as its buffer
  (`keep_absorbed` for a sort, `load_saved` for groups). The statement's runs
  stay in serial arrival order and its held records arrived after every run,
  which is the invariant `Runs::merge` relies on.
- A breaker whose groups cannot be saved, and `GROUP ALL`, do not spill in a
  batch; they stay in memory.
- A batch's spilled floor is dropped with the batch: a floor only prunes rows
  early.

Sealing writes a run the serial run might not have written. That changes no
result: the merge of runs in arrival order answers the same order as fewer,
longer runs.

**One run writer per query.** A query writes one run at a time
(`QueryMemory::writer`): a `RunSink` holds the query's single writer permit from
the moment it opens until it finishes or is dropped. The lease's headroom
reserves one writer's part buffer, `part_size * (1 + fan_in)`, and batches on
workers may spill concurrently; the permit keeps the headroom true. No code path
holds two sinks, and the statement awaits no batch while it holds one, so the
permit cannot deadlock.

### B.4 Fans

A fan is a process that runs the same program over a disjoint part of the
source and answers partials. The dispatch that starts a fan gives it the
program key, its producer index, and the coordinator's backend; how the fan is
started and how its `RunRef`s travel back is the dispatcher's concern (the
Cloudflare edge's fan-out), not this design's. This step supplies:

- `Partial::to_wire` and `Partial::from_wire`, big-endian throughout: the magic
  `SRLPARTL`, the run format and build key, the spec (the bytes a run's header
  carries from its program key through its order), `taken`, the runs, and the
  side records. Each run is a session name (u32 length, UTF-8), an id (u64), a
  tier (u8: 0 local, 1 shared), the bytes the run holds (u64, which a reader
  bounds the blocks it reads by) and its record count (u64). Each side record
  is a u64 length and the framed record. `to_wire` refuses a partial that holds
  records in memory or a run of the local tier, before it hands any run on; a
  refused partial's runs are deleted when it drops. `Run::into_ref` disarms the
  run's deletion and un-counts its bytes; `Run::adopt` counts them in the
  receiver's `QueryMemory`, which then owns and deletes the run. `from_wire`
  refuses another magic, format or build, and truncated, malformed or trailing
  bytes.
- `merge_partials`, and tests that run a program serially and as N simulated
  producers over one `MemTempStore` and compare results row for row, including
  ties, first values, and errors past the caps.

### B.5 Attaching to the plan's Spill label

The rule planner's physical plan carries a `Spill` label on breakers (on the
`vm/rule-planner` branch, `exec/vm/plan.rs`, not in this base). When it lands,
the label gains a merge kind:

```rust
enum PartialMerge { SortMerge { keys, keep }, KeyedFold, DistinctFirst, JoinBuild }
```

The label is static: it states that the breaker may spill and how its partials
merge. The compiler copies it into the breaker's spec (`SortSpec`,
`AggregateSink`, the distinct and join specs) as a `RunSpec` template; the VM
reads it at a buffer push and never consults the plan at run time. An aggregate
is labelled `KeyedFold` only when every accumulator it holds can save its
state, which is a property of the functions and known at compile time
(`Accumulator::can_save`, added with the label); an aggregate without it never
fans out and spills only as it does today.

## Step C — partitions of keyed breakers

### C.1 Decision: range partitioning by sampled splitters

A keyed breaker (aggregate with keys, GQL distinct, equi-join build and probe)
is divided into N partitions by **range**: N−1 splitter keys
`s_1 ≤ … ≤ s_{N−1}`, and a record with key `k` goes to partition

```
p(k) = |{ i : s_i < k }|      // partition_point over the splitters, by Value::cmp on the key vector
```

Correctness by construction: if `a == b` under `Value::cmp` and `Value::cmp` is
a total preorder, then for every splitter `s`, `s < a ⇔ s < b`, so `p(a) ==
p(b)`. Every key equal under the breaker's own equality lands in one partition,
and so does every record that could join, group, or deduplicate with it. No
property of the values beyond the order the breaker already uses is needed:
cross-type numeric equality (`1`, `1.0f`, `1dec`), strings, record ids, nested
arrays and objects all route by the same comparison that decides equality.

Why not a hash: a partition hash must satisfy `a == b ⇒ h(a) == h(b)` for
`Value::cmp` equality. `Value: Hash` does not (`0.1f == 0.1dec`, different
hashes). A canonical hash (map every number to its nearest `f64` and hash the
bits) is consistent for `Int`/`Float` and `Int`/`Decimal`, but
`Float`/`Decimal` equality is decided by a scaled `f64` comparison that is
neither exact nor transitive, so no canonical form provably agrees with it. No
existing dependency supplies one either. Range partitioning needs no hash at
all.

Residual: `Value::cmp` is not transitive where a `Float` and a `Decimal`
agree to about sixteen significant digits without being equal, so for such
keys there can be a splitter `s` with `s < a` and `!(s < b)` although
`a == b`. Main's `BTreeMap` is itself insertion-order dependent for such keys
(which of several mutually "equal" keys it finds depends on the tree's shape),
so no partitioning can promise main's exact grouping there, and a hash would
fare no better. The guarantee is exact wherever the keys present are totally
preordered by `Value::cmp`, which covers every `Int`/`Float`, `Int`/`Decimal`
and same-variant comparison. The equivalence tests pin `1`, `1.0f`, `1dec`,
`-0.0f`/`0.0f`, `0.1f`/`0.1dec` (equal, and routed together when no splitter
lies between them), and keys equal under `collate`/`numeric` sort flags but
distinct under `Value::cmp` (which group apart, as in main).

### C.2 Splitters

Splitters are drawn from **sorted runs**, which the breakers already write in
key order:

- a breaker that has spilled has runs in key order; the sampler reads every
  `stride`-th record's key while the runs are merged (or from the first run of
  each producer), yielding an ordered sample with no extra sort;
- a breaker that has not spilled samples its in-memory index (a `BTreeMap`/
  `BTreeSet` in key order) by stride;
- the coordinator merges producers' samples (each sorted) and picks N−1
  equally spaced keys, dropping duplicates (fewer partitions when the key space
  is narrow).

Splitter choice affects balance only, never correctness.

### C.3 Per breaker

- **Aggregate**: `Partitioned<Groups>`; partition `i` holds the groups whose
  keys route to `i`. Finishing partitions in index order answers the groups in
  key order, which is the order main finishes them in, so the output is the
  concatenation of the partitions' outputs.
- **GQL distinct**: a row's partition is its row's route. Partition `i` keeps
  each row's first arrival; output order is global arrival order, so the
  partitions' kept rows are merged by `(producer, arrival)` at the end (each
  partition's kept rows are written sorted by arrival, so this is a k-way
  merge, not a sort). The distinct-row cap is checked on the sum of the
  partitions' kept counts.
- **Equi-join**: build and probe rows route by the first `width` key columns
  (the arrival column appended to a build key is not routed on). Partition `i`
  joins its build rows with its probe rows; output order is
  `(probe arrival, place)`, restored by a k-way merge of the partitions'
  outputs, each written in that order.
  - Build rows whose key holds a regex are **replicated** to every partition:
    `=` matches them against string keys of any partition, after the equal-key
    matches, which is the order `BuildTable::matches` gives.
  - A probe row whose key holds a regex matches build rows of every partition,
    so a partition set that sees one marks itself `needs_global`, and the
    coordinator runs the unpartitioned spilled finish over the union of the
    partitions' build runs (the runs already exist; nothing is re-read from
    the source).
  - A cross join is not partitioned.
- **Sort**: not partitioned by this step. A sort's partials merge by step B.
  (Range partitioning a sort by its `KeyOrder` order is correct by the same
  argument and is left for a later parallel final merge.)

### C.4 Scope

Partitions are an API and a test-proven equivalence: `Partitioned<T>` with
`route`, `finish_in_order`, and the per-breaker merges above, exercised by
property tests that compare a serial run with an N-partition run over
generated inputs (mixed numeric variants, strings under every `KeyOrder`,
record ids with numeric and array keys, nested values, regex keys, ties, caps).
No runtime path partitions by default; single-node behaviour is unchanged. A
fan dispatcher that routes partitions to fans is the consumer.

## Backends

`TempStore` is the whole contract between the VM and spill storage, and nothing
in core is specific to one backend. Core ships the backends that work on a
single node (`FileTempStore`, `MemTempStore`, `TieredTempStore`); every other
backend (an object store, a Durable Object, a host's own storage) is an
implementation of the trait that lives with its host, outside this repository.
The trait states only what every backend must answer: write a run as a stream
of parts, read it back in order, delete it, name it with a `RunRef`, and say
whether another node can open it (`shares`). A backend sizes, buffers and
uploads parts however its storage requires; nothing in the VM depends on how.

## Testing

- **A**: header/trailer round trip; rejection of a wrong magic, format, build
  key, program key, a flipped byte in any block, a missing trailer, a count
  mismatch; a run written as `Shared` opened through a second handle on one
  shared `MemTempStore`; `Tiered` refusing to share local runs; `MemTempStore::most_read_at_once` never exceeding
  `fan_in`; top-k: a spilled `keep` sort answers the in-memory
  top-k for every `keep` around run and pass boundaries, with ties, and a pass
  writes no more than `keep` records (counted by the store).
- **B**: region with forced batch spill (tiny lease) answers the serial result
  for sort, sort with ties, `GROUP BY` with order-sensitive accumulators
  (`array::group`), first values, and `GROUP ALL`; the no-spill region path
  allocates as before (`vm_alloc` tests); N simulated fans over a shared store
  answer the serial result for every breaker, including caps' errors.
- **C**: the property tests of C.4.
