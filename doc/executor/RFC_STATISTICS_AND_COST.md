# RFC: Statistics and cost

**Status:** draft. Proposes the statistics the physical planner reads, how the
engine keeps them — as a materialization that writes maintain approximately
and a background refresh corrects — and the cost model that turns them into
the planner's choices. Statistics are hints: a stale, wrong or missing
statistic may make a plan slower and never makes it answer differently.

Companions: [`RFC_MATERIALIZATIONS.md`](RFC_MATERIALIZATIONS.md), whose
protocol keeps the statistics; [`RFC_DURABLE_BYTECODE_RUNTIME.md`](RFC_DURABLE_BYTECODE_RUNTIME.md),
whose triggered programs run the refresh;
[`PHYSICAL_PLAN_REQUIREMENTS.md`](PHYSICAL_PLAN_REQUIREMENTS.md), whose laws
(§1) and statistics requirements (§7) this note meets and whose open decision 2
it answers.

Code paths are relative to `surrealdb/core/src/` unless another crate is named.

---

## 1. What the planner decides today

The logical optimiser makes no choice with a cost: every rewrite is a strict
improvement, and which access path, join order or join algorithm to use is
left to the physical planner (`exec/ir/passes`).
The physical planner makes those choices from the plan's shape and the
catalog alone:

| decision | where | basis |
|---|---|---|
| which index, or a table scan | `select_access_path` (`exec/index/access_path.rs`), over the candidates of `IndexAnalyzer::analyze` (`exec/index/analysis.rs`) | `WITH NOINDEX` scans; a KNN candidate drives; a `WITH INDEX` name wins; otherwise the highest `IndexCandidate::score`: `EmptyScan` `u32::MAX`, unique equality 1000, full-text and KNN 800, equality 500, compound prefix 400 + 50 per column (capped at six) + 25 with a range, bounded range 300, half-bounded 200, unbounded 50, and +100 when the index covers `ORDER BY` |
| index against table scan | `resolve_access_path`, reached through `Planner::decide_access_path` (`exec/planner/select/mod.rs`) | a table scan is chosen only when no candidate exists. Any applicable index is used whatever fraction of the table it returns |
| bitmap fusion, OR fusion | the same function | no hints, no `VERSION`, no index covering `ORDER BY`, and not `LIMIT` without `ORDER BY` |
| a multi-index union, `IN` or containment expansion | the same function | taken when no single candidate exists, or when the best candidate is a full-range scan; a nested OR's union when its weakest branch outscores the best single candidate |
| lookup join or hash join | `Planner::compile_join_parts` (`exec/planner/fold.rs`) | a lookup join when the one key pair is `(left key, alias.id)`, the right input is a table chain, and nothing on the right may raise; otherwise a hash join that builds the right side |
| join order, build side | the lowering and `exec/planner/match_plan.rs` | as written: a decorrelated join's left input is the outer pipeline and its right the subquery; GQL patterns and clauses fold left to right |
| a GQL pattern's anchor | `plan_pattern_selfrooted` (`exec/planner/match_plan.rs`) | a labelled start node, else a labelled edge, else a bound variable |
| the fast paths | `exec/planner/select/fast_path.rs` | shape only |
| buffering | `CardinalityHint` (`exec/cardinality.rs`) | a static bound from the operator, not an estimate of the data |

The VM takes its access-path decision through `Planner::decide_access_path`, called from
`exec/vm/map.rs`, so there is one decision.

Two runtime guards bound the damage a bad plan does and stay whatever this
RFC adds: a bitmap branch that drains more than `BITMAP_BRANCH_BUDGET` entries
abandons the bitmap for its streaming fallback (`exec/operators/scan/bitmap.rs`,
`surrealdb/cnf/src/lib.rs`), and a plan-time record fold stops at its fetch
budget (`exec/planner/select/record_fold.rs`).

### 1.1 What the storage keeps

Only exact structures, each read for answers:

- **A `COUNT` index** writes one key per committing transaction,
  `!iu{uid}{pos}{count}` under the index (`surrealdb/datastore/src/key/keyspace.map`), carrying
  the delta in the key. A read sums the keys and adds its own transaction's buffered delta
  (`surrealdb/idx/src/count.rs`); the index compaction queue folds the keys under a generation
  compare-and-set (`kvs/ds.rs`). No two transactions write the same key, so the count never
  contends.
- **A full-text index** keeps its document count and total length the same way
  (`surrealdb/idx/src/ft/fulltext.rs`).
- **A table's row count** exists only as a prefix count, `O(n)` (`surrealdb/kvs/src/api.rs`).

### 1.2 No cost model on the query path

No statistics type, cost model or physical IR with costs is on the path a query
takes, and no storage keeps a table or index statistic beyond the counts above.
§9 gives the cost terms and starting weights this RFC begins from.

## 2. The invariant: statistics are hints

**A statistic never changes an answer.** Rows, their order where the statement
specifies one, errors and evaluation counts are identical under any
statistics, including none (requirements §1, law 1). Therefore:

- **Nothing reads a statistic for an answer.** `count()`, `INFO` totals, a `COUNT` index's answer
  and every fast path answer from exact structures. A statistic may be approximate because nothing
  depends on its being exact.
- **The planner is complete without statistics.** Every choice has a no-statistics default, and
  the defaults are the current heuristics of §1. A plan compiled with every statistic unknown is
  the plan compiled today.
- **Unknown is a value.** The interface answers "unknown" for an absent statistic, one still
  building, and one the caller may not use (§5), so a rule never mistakes an absent count for zero.
- **Cost chooses only among equivalent plans.** A choice that changes whether a statement raises
  or which fields are read is gated before it is costed (law 6, §5.2 there): KNN and `MATCHES`
  bind their index at mapping, `WITH INDEX` and `WITH NOINDEX` restrict the candidates, an index
  over a column the caller may not read is withheld (`drop_restricted_indexes`), and a lookup join
  is taken only where the right side cannot raise. Cost reorders what remains.

The invariant is what lets a statistic exist at all. Every write to a table
changes its statistics, so a statistic is inside the static write footprint of
every statement that writes the table, and the memo rule (requirements §9.6)
forbids a statement to reuse a value its own control graph may write. A
statistic escapes that rule only because no evaluation reads it: the planner
reads it once, before execution, from committed state, excluding the
transaction's own buffered deltas (unlike a `COUNT` index read), and nothing
downstream holds it.

For the same reason a statistic read at plan time does not break law 3
("nothing reads data at plan time"): that law keeps records out of a plan's
dependencies, and a statistic is not a dependency (§7).

## 3. What is collected

A statistic is collected only when a decision of §6 reads it.

| statistic | per | decision it informs | kept by |
|---|---|---|---|
| **row count** | table | index against table scan; build side; lookup against hash join; the fan-out denominator | write deltas (§4.2) |
| **entry count** | B-tree, unique and array-element index | the same, per index; entries per record for an array-element index | write deltas |
| **distinct prefixes** *d*₁…*d*ₖ | B-tree index of *k* columns | equality and compound-prefix selectivity (*entries* / *d*ᵢ); join cardinality on an indexed key | refresh (§4.3) |
| **histogram and most-common values** | B-tree index, leading column | range selectivity; equality on a skewed value | refresh |
| **edge count** | edge table, endpoint table, direction | fan-out of a graph step (*edges* / *rows of the endpoint table*): GQL anchor and expansion order, a traversal against an edge-table scan, hash-join sizing | write deltas |

Distinct prefixes come from an ordered scan of the index's keys: the number of
distinct values of the first *i* columns is the number of positions where the
first *i* columns change, so one pass gives all *k* without a sketch. A unique
index has *d*ₖ equal to its entries and needs no pass. The histogram is
equi-depth, its bounds every *entries* / *B* keys of the same pass, held as
encoded index-key bytes so that they compare as the index does; the
most-common values are the keys whose runs exceed one bucket.

Edge counts need no extra read: a `RELATE` or an edge delete knows its edge
table and both endpoint tables, and writes a delta for each (edge table,
endpoint table, direction). The fan-out of a step is the expected number of
edges per record of the endpoint table, records with no edge included, which
is what the cost of expanding a row needs.

Omitted, because no decision here would read them:

- **Tree height.** The index is a range of an ordered KV store; a seek costs a round trip, not a
  tree walk the planner can see.
- **Partitions.** Key-range boundaries are the backend's topology, read by fan-out planning from
  the backend; they are not maintained from records.
- **Average record size.** Decode cost per record is taken as constant (§10, question 3).
- **Distinct values of an unindexed field.** Useful for a hash join on an unindexed key, but
  keeping it needs a sketch such as HyperLogLog, and a sketch cannot forget a deleted value. Any
  sketch library is a new dependency and needs approval (§10, question 2); without one, such a key
  is unknown.
- **Full-text and vector statistics.** `MATCHES` and KNN bind their index at mapping, and the
  full-text index already keeps what its scoring reads.
- **Record-link fan-out** (the length of an `array<record<t>>` field) — deferred to §10, question 4.

## 4. Maintenance

### 4.1 An approximate materialization

The statistics of table *t* are a materialization *S* of *t*
(materializations RFC §2) with a derived target in its own band. Its
transform *T*(before, after) produces deltas: −1 to the row count for a
before-image and +1 for an after-image, the entry-count delta the index
maintenance computes, and the edge-count deltas of an edge record. *T* reads
the row only, cannot raise, and is called in the writing transaction like any
materialization's (materializations RFC §2), so a rolled-back write leaves no
delta.

*S* departs from an exact materialization in one respect: it is maintained
without marks and without the barrier (materializations RFC §4.2, §4.4). Both
exist to make a target exact, and they cost a key per record and a
cluster-wide wait. Without them a record written into a range the backfill has
not yet reached is counted by the writer's delta and again by the backfill,
and one deleted there is counted by the writer's delta and missed by the
backfill: the count errs by at most the net writes to the unscanned range
while the backfill runs. The first refresh after `Online` removes that error
(§4.3). *S*'s read policy is neither gated nor read-through
(materializations RFC §6): it is **advisory**, read in any state, reporting
unknown until *S* first goes `Online`.

*S* exists for every table and is defined with it. Defining an index adds its
statistics to *S*; nothing about them delays the index's own `Online`.

### 4.2 Deltas, and the shared-state footprint

Every write to *t* changes *t*'s row count, so a single counter key would be
shared state that every writer read-modify-writes — the footprint the
materializations RFC serialises (§3.1 there) and asks about for aggregates
(§10, questions 4 and 5). A statistic is instead written as a **delta key
unique to its writing transaction**, the `COUNT` index's mechanism (§1.1):
the key names the transaction, and the delta is in the key. Two writers never
write a common key, so they never conflict, and *T*'s footprint is declared
**commutative**: effects that sum, whose order is irrelevant, and which the
planner may fan out across batches as freely as per-record effects. A
statement batching many rows writes one delta key per statistic, not one per
row.

A reader sums the base and the delta keys since it. A **fold** — the index
compaction queue's job, extended to *S* — replaces the base and the deltas it
read with their sum, fenced by a generation as `IndexCountCompactionPlan` is.
The fold is the only writer of the base; it conflicts with nothing a writer
writes.

Commutative deltas also answer what the materializations RFC leaves open for
a `COUNT` total and for an aggregated view's sums and counts: those are the
same shape. Posting lists and graph neighbourhoods are not, and remain its
question 4.

### 4.3 The refresh

What deltas cannot keep — distinct prefixes, histograms, most-common values —
is rebuilt by a **refresh**: a triggered program (runtime RFC §4) that scans an
index's keys in order, in batches, checkpointing its cursor and running
accumulators with each batch, and writes the result in one transaction at the
end. It reads keys only and decodes no record. A refresh of *t* also re-counts
the rows and entries it scans and rebases the counts on them, which removes
the error of §4.1.

A refresh is triggered:

- when *S* goes `Online`;
- when the counts have moved by a fraction ρ since the last refresh, observed by the fold, which
  alone sees the deltas' sum change;
- by `ANALYZE TABLE t [INDEX i]` (proposed), which triggers the refresh and returns once its
  invocation is recorded, or with `WAIT`, once it completes.

A refresh runs at background priority, one per table at a time; a trigger
arriving while one runs is dropped.

### 4.4 Staleness

- **Counts** lag by the deltas a reader does not read. A reader reads at most *D* delta keys; past
  *D* it takes the base plus those it read, enqueues a fold and treats the estimate as stale for
  §7. The error of a count is therefore bounded by the unread deltas, and the read by *D*.
- **Refreshed statistics** carry the counts they were computed at. A reader scales them to the
  current counts (distinct prefixes proportionally, clamped to the entry count; histogram
  fractions unchanged), and they are stale past ρ, which triggers a refresh.
- **Nothing expires by time**. A table nobody writes keeps statistics that remain true.

## 5. Permissions

A statistic is computed over every record of a table, whoever may read them.
Exposing one to a caller who cannot read every record discloses how many
records it cannot see, and how their values are distributed. The same holds
for anything a statistic decides: the plan a statistic chose is observable
through `EXPLAIN` and through timing. The engine already withholds on this
ground an index over a column the caller may not read (`drop_restricted_indexes`,
`exec/planner/select/mod.rs`) and a `COUNT` index when the condition touches a
restricted field (`cond_touches_restricted_field`, `idx/planner/tree.rs`).

- **Reading a statistic.** Statistics are shown by `INFO FOR TABLE` and `INFO FOR INDEX` (proposed
  sections), which already require `View` on the database (`exec/operators/info/table.rs`,
  `exec/operators/info/index.rs`), and in `EXPLAIN` estimates, which are printed only for a plan
  not compiled for a checked caller (`Planner::compiles_for_checked_caller`, `exec/planner.rs`).
  For a checked caller `EXPLAIN` prints no estimate anywhere in the plan; `EXPLAIN ANALYZE`'s
  actual counts keep their existing redaction beneath enforcing nodes (`SECURITY_GUIDE.md` §2).
- **Choosing with a statistic.** A plan compiled for a checked caller consults a statistic of
  table *t* only when the caller could compute it: *t*'s select permission is `FULL`, and for an
  index statistic every column of the index has select permission `FULL` (which index eligibility
  already requires). An edge count needs both the edge table and the endpoint table to qualify.
  Otherwise the source is planned as with every statistic unknown. This covers every source the
  plan holds, in the caller's frame or a permission frame: the caller observes the timing of both.
- **Refreshing.** `ANALYZE` writes, and requires the rights `REBUILD INDEX` requires.

Whether a statistic was consulted is decided from the permission regime and
the table's and fields' permissions, all of which are in the plan's cache key
(requirements §6.2), so a cached plan never carries a statistic-driven choice
to a caller who could not have made it.

## 6. The cost model

### 6.1 What is costed

A candidate's cost is a weighted sum of:

| term | counts | dominant on |
|---|---|---|
| **rows out** | rows the candidate hands upward, which every operator above pays for | every backend |
| **KV reads** | range seeks, keys read in ranges, point and batched gets (`RecordBatchFetch`) | local backends |
| **decodes** | records decoded, less the fraction the pre-decode filter rejects (`exec/pre_decode_filter/`) | local backends |
| **round trips** | seeks, batches of a range scan and batched gets | remote backends (TiKV) |
| **memory** | rows held by a hash build or a blocking sort | every backend; decides spilling |

The weights are per backend, measured on the bench suite (`surrealdb/core/benches/`)
rather than argued. A plan's cost is compared only against its alternatives
for the same statement on the same backend, so the unit is immaterial.

### 6.2 Cardinality

With *N* the row count and *E* an index's entries:

- **Equality** on an indexed value: its count when it is a most-common value, else
  (*E* − Σ most-common) / (*d* − |most-common|); a unique index, at most one. A compound prefix of
  *i* columns: *E* / *d*ᵢ.
- **A range**: the histogram fraction between its bounds, times *E*.
- **An array-element index** returns entries, not records: records ≈ entries × *N* / *E*.
- **A conjunct no statistic answers**: a fixed selectivity, the same constants for every
  conjunct of a kind. Conjuncts combine as independent: `AND` multiplies, `OR` is 1 − Π(1 − *s*),
  and every result is clamped to [1, *N*].
- **An inner equi-join**: |*L*| × |*R*| / max(*d*_L, *d*_R), with each *d* taken from an index on
  the key where one exists and otherwise unknown; a semi or anti join, at most |*L*|; a graph step,
  |*L*| × fan-out.
- **`LIMIT k`** over a stream in the required order with residual selectivity *s*: the stream
  reads min(*E*, *k* / *s*) entries before it stops.

An estimate from an unknown input is unknown, and a decision with an unknown
input takes the no-statistics default.

### 6.3 Choosing

- **Access paths.** For each table source, cost every candidate the analyser produces — each
  index candidate, the unions and expansions, bitmap fusion, and the table scan — and take the
  cheapest. The table scan's cost is *N* keys, *N* decodes, and ⌈*N* / *b*⌉ round trips; a
  non-covering index scan's is its entries, the same number of batched gets and decodes, and
  their round trips. The fetch per record is what makes an index lose to a scan when it returns
  most of the table, which §1 never does.
- **Ordered scans.** An index scan covering `ORDER BY` avoids the sort; under `LIMIT` it reads
  *k* / *s* entries. Without `LIMIT` it reads the whole index and fetches every record, and is
  compared against a scan plus a sort. This replaces the +100 order bonus.
- **Lookup or hash join.** Where both are legal (§2), a lookup costs |*L*| batched gets; a hash
  join costs the right side's plan, its build and |*L*| probes.
- **Build side and join order.** Swapping inputs changes the output's order, which a join keeps
  from its left input, and whether the right side is evaluated when the left is empty (the build
  waits for the first probe row). A swap or a reorder is therefore considered only for inner
  joins, in a statement with no `ORDER BY` over the join, whose inputs cannot raise
  (`plan_may_raise`). Among those, a join region of up to *J* inputs is ordered exhaustively over
  connected subsets, cheapest intermediate result first; past *J*, greedily, joining the pair
  with the smallest estimated result next. Cross products come last.
- **GQL anchors.** A pattern with more than one labelled element anchors on the element whose
  scan and expansion cost least, where the anchor is legal (`match_plan.rs`'s anchorability rule).

A choice with no statistic takes the default of §1. Cost never overrides a
semantic gate of §2 or a hint.

## 7. Plan caching

A cached plan is keyed by the statement, the permission regime and the version
or content hash of every catalog object it read (requirements §6). Statistics
are not in the key: a plan chosen on any statistics is correct, so a stale
plan is slow, never wrong (law 1).

A plan instead records its **basis**: for each table whose statistics it
consulted, the statistics' generation. The fold and the refresh advance a
table's generation only when a count has crossed a power of two since the last
advance, or a refresh has written new distributions; smaller movement leaves
it, which gives the plan cache hysteresis and keeps a plan from flapping
between two near-equal choices. On fetch, the cache compares the plan's basis
with the current generations — per-table values held node-wide, as catalog
snapshots are (`surrealdb/datastore/src/cache/ds/catalog.rs`) — and re-plans on
a mismatch. The re-plan may choose the same plan.

A cached plan compiled for a checked caller records no basis for a table §5
barred it from consulting, and so is never re-planned for that table's
statistics.

A parameter substituted at binding gives a literal the estimate can read; a
plan reused across parameter values must be chosen on a value-independent
estimate (§10, question 5).

## 8. What is durable

- *S*'s state record, as every materialization's (materializations RFC §8).
- The delta keys, the base, and the generation, in *S*'s band.
- Per index, the refresh's result: the counts it was taken at, the distinct prefixes, the
  histogram bounds and the most-common values with their counts, the bounds and values held as
  encoded index keys.
- The refresh's invocation and checkpoint, as the runtime RFC defines them.

The band lies under the table, so `REMOVE TABLE` and `REMOVE INDEX` reclaim it
with the table's and the index's keys. Export does not write statistics, and
an import rebuilds them by the backfill of *S*.

Each of these is a persisted format, and `CLAUDE.md` requires upgrade pairs in
`language-tests/tests/upgrade/` for each as soon as a release writes it:

- a table written by a release with no statistics, read on the first release with them: the same
  answers, statistics unknown until *S*'s backfill completes, then the counts `INFO FOR TABLE`
  reports;
- statistics written by release N, read and maintained by N+1, with the count `INFO` reports
  after writes on both;
- a refresh or a backfill in progress under N, resumed by N+1.

Because a statistic cannot change an answer, the answers of an upgrade pair
cannot show a lost statistic. The pairs therefore read the statistics through
`INFO`: without that, an unreadable format would fall back to unknown and every
test would pass, the silent failure `CLAUDE.md` warns of.

## 9. Cost terms and starting weights

- **Cost components.** A plan's cost has four components, `io`, `cpu`, `memory` and
  `coordination`: the terms of §6.1 grouped by resource. KV reads and round trips go into `io`
  and `coordination`, decodes and evaluation into `cpu`, builds and sorts into `memory`.
- **Starting calibration.** Fixed weights for a point get, a batched get, a full decode, a hash
  build and probe, and a coordination leg are the starting point that the benches then correct.
  An index's point selectivity and average entries per key are §6.2's equality estimate without
  most-common values.
- **Unknown is a value.** No statistic is required. A missing one is unknown, never a substituted
  fixed number, and the §1 heuristics are the default (§2). Tree height and partition counts are
  not kept (§3); quantiles are the histogram of §3, held as index keys rather than `Value`s.
- **Record fetches are costed.** Fetching the record of a non-covering index entry is a charged
  term: §6.3's choice between index and scan turns on it.
- **Costing runs over live choices.** The costing of §6 runs over what the live planner chooses
  among: `AccessPath` candidates (`exec/index/access_path.rs`), the join forms of
  `compile_join_parts`, and GQL anchors.

## 10. Open questions

1. **ρ, *D*, *B* and *J*.** The refresh threshold, the most delta keys a reader reads, the
   histogram's buckets and the exhaustive join-order bound. Proposed starting points — ρ = 0.2,
   *D* = 64, *B* = 64, *J* = 6 — are to be measured, not argued.
2. **A sketch.** Distinct values of an unindexed join key, and a cheaper refresh of a very large
   index, want a mergeable sketch such as HyperLogLog. Is a dependency wanted (it needs approval),
   or an in-tree implementation, or neither until a measured plan needs it?
3. **Record size.** A table of large records makes a scan dearer relative to an index than one of
   small records. Is an average encoded size, kept by deltas as the counts are, worth its write?
4. **Record-link fan-out.** The length of a link array decides the cost of a lookup over it, but
   is a property of records, not keys, so a refresh would decode them. Sampled, or unknown?
5. **Parameter sensitivity.** A plan cached across parameter values cannot use a most-common value
   of the one it was compiled with. Plan on the average estimate always, or key the cache by
   whether the value is a most-common value?
6. **Sampling large indexes.** A keys-only scan of a very large index is still `O(entries)`.
   Backends offer approximate range sizes and split keys (TiKV's regions); a refresh could sample ranges from them. Is the resulting error acceptable for
   histograms?
7. **Conditional permissions.** Under §5 a record user on a table with a `WHERE` select permission
   always gets the no-statistics plan. Is that acceptable, or should statistics be kept per
   permission predicate, which multiplies what writes maintain?
8. **The key bands** for delta keys, bases and refresh results, shared with the materializations
   RFC's question 7.

## 11. Order of work

1. **Row counts, entry counts, and index against scan.** The statistics interface with unknown as
   a value; *S* with row and entry counts by deltas, its fold on the compaction queue, and the
   backfill; §6.3's costing of access paths only. Measured on the bench suite — first
   `select_table_index_noindex` in `benches/executor.rs`, whose `field > 4999` returns half of
   10,000 records and is the case §1 plans with the index — on the memory and TiKV
   backends. The language suite runs with statistics absent and with
   statistics replaced by random values, and each answer must equal the run without statistics,
   which is §2 checked rather than trusted. The permission rule of §5 and its tests; the upgrade
   pairs of §8 for the counts.
2. **Refreshed distributions.** Distinct prefixes, histograms and most-common values; `ANALYZE`;
   equality, compound and range estimates; ordered scans against scan-plus-sort.
3. **The plan cache's basis** and generations (§7), with the cache the requirements define.
4. **Joins and graphs.** Edge counts; lookup against hash; build side and join order under §6.3's
   gates; GQL anchors.
5. **`INFO` and `EXPLAIN`** estimates, under §5.
