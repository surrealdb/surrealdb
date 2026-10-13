# Physical plan requirements

What the physical plan has to express so that it can be mapped from the logical plan, optimized
in place, cached, lowered to the bytecode VM and fanned out across threads, without changing an
answer the language gives today. Each requirement names the code or the language test it rests on.
Code paths are relative to `surrealdb/core/src/` unless another crate is named; test paths are
relative to `language-tests/tests/`, with `repro/` for `reproductions/`.

The pipeline this constrains:

```
LogicalOp ──map──▶ PhysicalPlan ──optimize (IR → IR)──▶ PhysicalPlan ──lower──▶ bytecode
          catalog                  catalog, statistics                 (no decisions)
                                                         └── cached, keyed by what it read
```

- **Map** translates each logical node into its physical counterpart, reading from the catalog
  only what a node needs to exist: the table, its fields, its indexes, its permissions.
- **Optimize** makes every choice (access path, pushdown, top-k, placement, fan-out) as rewrites
  over the same representation. Rules may read the catalog and statistics.
- **Lower** turns the plan into a VM program and makes no choices. A statement holding a node the
  VM does not cover fails with an error that names it (ADR 0007).

The language suite's expectations are the reference. What the physical plan owns, and what it may
only narrow of what the logical plan allows, is stated in [`PLAN_LAYERS.md`](PLAN_LAYERS.md).

---

## 1. Laws

Every rule, every node and every lowering is bound by these. A rule that cannot satisfy one of them
is not a rule.

1. **Statistics never change an answer.** Rows, their order where the statement specifies one
   (§4), errors and evaluation counts are identical under any statistics, including none.
   Statistics choose between plans that are equivalent under that definition, and a plan without an
   `ORDER BY` may therefore return its rows in a different order as statistics change.
   Consequently statistics are not part of the cache key; a plan whose estimates have drifted is
   re-planned for speed, never for correctness.
2. **Every catalog read is a dependency.** Mapping and optimization read the catalog only through a
   recorder that adds the object and its version to the plan's dependency set. A read that has no
   version to record (see §6) makes the plan uncacheable until it has one.
3. **Nothing reads data at plan time.** A plan depends on definitions, never on records. The
   planner's record fold (`exec/planner/select/record_fold.rs:191`, `get_record`) violates this and
   becomes a runtime operation or is dropped from the cacheable path.
4. **Evaluation counts are preserved.** An expression is evaluated as many times, in the same
   evaluation class (§3), and in the same order relative to anything that can raise, as the
   language specifies. A rewrite may remove an evaluation only of an inert expression
   (`AST_TO_EXECUTION.md`, "What a rewrite may do to an evaluation").
5. **The plan is inert and principal-independent.** It holds no transaction, cursor, future or
   session value. The principal enters the plan only as a permission regime: checked, or bypassed
   for a root/owner caller (ADR 0003). Identity (`$auth`, `$session`, `$token`) is a runtime
   input.
6. **Access paths are not interchangeable by default.** Changing an access path can change
   whether a statement raises and which fields are read. Each access-path rule carries the semantic
   gate of §5.2, not only a cost comparison. The order it delivers matters only where §4 specifies
   one.
7. **Writes are sequential.** Within one statement, subjects, stages, observers and savepoints run
   in order on one task. Only read-only subtrees fan out (§8).

---

## 2. Node catalogue

### 2.1 Read

| node | carries | semantics it must preserve | evidence |
|---|---|---|---|
| `Scan::Table` | table, existence obligation, permission regime | a missing table raises `TbNotFound` on `SELECT` in any mode and at `LIMIT 0`; a table whose select permission is `NONE` yields no rows | `exec/operators/scan/table.rs:277`, `dbs/iterator.rs:346,372`; `language/statements/select/missing_table_limit_zero.surql` |
| `Scan::Point`, `Scan::Range` | table, key or bounds, existence obligation | a missing record is no row; a range is in key order | `exec/operators/scan/record_id.rs:357`; `repro/recordid_range_start_limit.surql` |
| `Scan::Mock` | table, bounds | reads records, it is not a literal; missing table raises | `dbs/iterator.rs:451` |
| `Scan::Values` | expression | array → one row per element, NONE/NULL → none, other → one; a record-id element is read with permissions and computed fields **before** any pushed predicate, limit or start; the row may not be an object | `exec/operators/scan/dynamic.rs`; `repro/a_zero_limit_still_reads_the_record.surql`, `repro/a_value_source_is_not_an_object.surql` |
| `Scan::Dynamic` | expression naming a table | the table is resolved at run time, so its catalog is not a plan dependency and nothing that needs it is decided at plan time | `exec/operators/scan/dynamic.rs` |
| `Scan::Lightweight` | edge table | edges come from adjacency keys and the record is synthesised; table permissions apply to it | `exec/operators/scan/lightweight.rs` |
| `Scan::Index`, `UnionIndex`, `Bitmap` | index id, bounds, delivered order | rows equal the table scan's; an index over a field whose select permission is not `FULL` is not eligible | `repro/restricted_field_index_oracle.surql`, `repro/codex_union_index_field_perm_leak.surql` |
| `Scan::FullText` | index binding per match ref | see §5.2 | `exec/planner/select/mod.rs:905-945` |
| `Scan::Knn`, `KnnTopK` | index binding, k, residual | residual `WHERE` applies **before** ranking | `exec/operators/knn_topk.rs`; `repro/knn_matches_zero_rows.surql` |
| `Scan::Count`, `IndexCount` | source, count index | answer equals the scan's; never under `VERSION` | `exec/operators/scan/count.rs`; `repro/count_permission_parity.surql`, `repro/a_versioned_count_reads_the_stamp.surql` |
| `Traverse` | graph path | materialises records, so it is an effect; `LIMIT` counts rows visible after permission filtering | `exec/operators/scan/graph.rs`; `language/graph/limit_pushdown_permissions.surql` |
| `PermissionFilter` | predicate, frame | beneath every user stage; no user expression moves below it; a conditional permission is evaluated per admitted row even if it folds | `exec/logical/resolve.rs`; `repro/permission_where_true_is_conditional.surql` |
| `ComputeLayer` | computed fields, guards, frame | order: table permission, computed layers, stored-field guards, re-derivation, computed fields' own guards | `exec/logical/resolve.rs`; `repro/computed_field_reads_restricted_source.surql` |
| `Filter` | conjuncts, each with `may_raise` | table permission judged first; conjuncts reached left to right; none moves ahead of one that can raise | `repro/where_permissions_data_leak.surql`, `repro/a_pushed_conjunct_keeps_its_place_behind_a_raising_one.surql` |
| `Compute` | named expressions shared downstream | a sort key that is a projection alias is evaluated once and reused by the projection | `exec/expression_registry.rs`; `language/statements/select/order/order_by_nondeterministic_alias.surql` |
| `Split` | paths | sits **above** `Project` and divides the projected row, although the logical plan places it above `Filter` | `exec/planner/select/pipeline.rs:337`; `repro/split_names_a_field_of_the_projected_row.surql` |
| `Aggregate` | keys, aggregates, the projection it owns | groups in key order; `GROUP ALL` over nothing is one row of empty aggregates, `GROUP BY` over nothing is none; extra aggregate arguments evaluate once per group | `exec/operators/aggregate.rs`; `repro/group_all_over_no_row_is_a_grand_total.surql`, `repro/a_group_argument_is_drawn_once_per_group.surql` |
| `Sort`, `TopK`, `ExternalSort`, `Shuffle` | keys `{expr, asc, collate, numeric}` or random | stable: arrival position is the last key on every variant; `TEMPFILES` chooses the variant and never the answer | `exec/operators/sort/external_common.rs:23`; `language/statements/select/order/order_ties_stable_with_limit.surql` |
| `Limit` | start, limit expressions | each evaluated once per statement; paging follows the predicate, which is judged even at `LIMIT 0` | `repro/a_limit_clause_is_evaluated_once.surql`, `repro/paging_does_not_skip_the_predicate.surql` |
| `Project` | fields, omit paths, `VALUE` | a projection is not evaluated for a missing record; `OMIT` paths are structural | `repro/select_skip_field_eval_on_missing_record.surql`, `repro/omit_paths_that_need_evaluating.surql` |
| `Fetch` | paths | denied or missing record → `NONE`; missing table raises; not a left join | `exec/operators/fetch.rs:524-560` |
| `Join::Hash`, `Join::Lookup` | keys, residual, frame | keeps the left input's order; the build side is built only once a probe row exists | `repro/join_empty_probe.surql` |
| `Timeout` | deadline expression | cast to a duration before the input opens; expiry aborts with `QueryTimedout` | `exec/operators/timeout.rs:172`; `repro/a_timeout_clause_is_read_before_the_work.surql` |
| `VersionScope` | stamp expression | evaluated once, outside the scope; everything beneath reads that stamp, including nested pipelines and function bodies; no index or count path beneath it but a full-text scan (a `MATCHES` has no evaluation but its unstamped index, which answers it for now while the records it names are read at the stamp), and an approximate KNN is ranked over the records read at the stamp | `exec/operators/version_scope.rs`; `repro/nested_pipeline_inherits_version.surql`, `repro/a_versioned_btree_read_matches_its_noindex_twin.surql`, `repro/a_versioned_order_and_limit_read_the_stamp.surql`, `repro/a_versioned_knn_ranks_the_records_at_the_stamp.surql`, `repro/a_versioned_full_text_match_reads_the_current_index.surql` |
| `Only` | — | 0 rows → `NONE`, 1 → the value, more → `SingleOnlyOutput` | `exec/operators/unwrap_exactly_one.rs` |
| `Explain` | analyze flag | no rewrite runs through it; under a checked regime, definer text and metrics beneath enforcing nodes are redacted | `repro/layers_explain_redaction.surql` |

The tail order after lowering is `Filter → Aggregate | Split-over-Project → Sort → Limit → Project
→ Fetch → Timeout → VersionScope → Only → Explain` (`expr/src/expr/logical/lower.rs:341-466`,
`exec/planner/select/mod.rs:559-583`).

### 2.2 Write

The write node is not a composition of relational operators. Its per-record behaviour is a fixed,
ordered list of stages, and the plan carries that list as data (ADR 0001 §2).

| node | carries | evidence |
|---|---|---|
| `Write` | form; facts (`has_data`, `on_duplicate_key_update`, `ignore`, `repeatable`); the ordered stage lists; branching (`Sole`, `UpFront`, `Retry`) and the savepoint split; data clauses; output (`NONE`, `BEFORE`, `AFTER`, `DIFF`, projection); `ONLY`; import flag; permission regime | `exec/operators/write/plan.rs:94,205,673-972`; `exec/operators/write/stage.rs:133,294` |
| subject source | `Targets`, `Edges` (RELATE from × to), `Inserts`, or `Rows(plan)` | `expr/src/expr/logical/lower.rs:100-220` |
| `Observer` | a nested, writing, catalog-derived plan: a view order, an event body, a reference cascade, an edge cascade | `exec/operators/write/stages/{view,observe,purge}.rs` |
| `GuaranteedRecord` | the post-loop step of `UPSERT … WHERE`, driven by a match counter | `exec/operators/write/mod.rs:927-988`; `language/write/guaranteed_record_is_per_statement.surql` |

What the write node must preserve:

- **Stage order.** Twelve orders, from 46 stage entries. Permissions are checked both before and
  after the data clause, and every `ASSERT` runs after all fields are produced. A model with
  fewer, fixed stages cannot express this order and is not a basis.
- **Subject order.** Targets in the order written; multi-target `UPDATE`/`DELETE` is a sequential
  `UnionAll`; the source must preserve order.
- **Per-row, exactly-N evaluation.** `WHERE`, data clause, permission predicates, field clauses and
  `SET` subqueries evaluate once per subject at their stage. The update permission runs exactly
  twice per edited record (`language/write/update_permission_evaluated_twice.surql`). `DELETE`
  runs the select gate against the pre-image even without `RETURN`
  (`language/write/delete_runs_the_select_gate.surql`).
- **One pre-mutation snapshot per subject.** Every data-clause expression reads the same
  field-reduced pre-image (`language/write/data_clause_reads_pre_mutation_snapshot.surql`).
- **Fresh reads see earlier writes.** A subquery in `SET` sees every write the statement made
  before it, including what observers wrote
  (`language/statements/update/set_subquery_sees_earlier_writes.surql`). `INSERT` evaluates every
  row before writing any.
- **The scan does not see what the statement wrote** (`language/write/self_insert_not_reobserved.surql`).
- **Unique violations raise eagerly** in `StoreIndexData` (`surrealdb/idx/src/index.rs:444`); the
  `UPSERT`/`INSERT` retry depends on it.
- **Atomicity.** A failure on any subject aborts the statement; `SkipRow` is the only non-failing
  exclusion (`language/write/per_record_failure_aborts_statement.surql`).
- **Observers.** Views and sync events run after the store, in the same transaction, sequentially.
  Async events are queued in the transaction and drained after commit. Live notifications and
  changefeeds are buffered until commit. All are suppressed under import and when the post-image
  equals the pre-image.

`WHERE` on a write is not pushed into the source (`exec/logical/rules/filter.rs:563`). Pushing it
into an index scan is a physical rule gated on permissions being statically absent for the form,
and on the side-effect count still equalling the match count.

### 2.3 Control flow

`Sequence`, `Let`, `Foreach`, `If`, and the signals `Return`, `Throw`, `Break`, `Continue`. A
statement after a catalog write in the same block that the plan cannot model is planned late-bound
when the query arrives, loading each catalog object it names when it runs (`exec/vm/map.rs`,
`Blocks::late_after`). A signal is not a row; a column that signals is not re-evaluated.

---

## 3. Evaluation classes

Every expression slot in the plan is in exactly one class, and lowering emits it at the matching
point in the program.

| class | examples | evidence |
|---|---|---|
| per statement | `LIMIT`, `START`, `TIMEOUT`, `VERSION`; `time::now()` in a `WHERE` | ADR 0006; `repro/a_limit_clause_is_evaluated_once.surql` |
| per group | extra aggregate arguments | `repro/a_group_argument_is_drawn_once_per_group.surql` |
| per row | predicates, projections, sort keys, data clauses, permission and field clauses | `repro/a_projected_effect_happens_once_per_row.surql` |
| per call | clock reads outside a `WHERE`, entropy, external calls | `language/functions/time/now_evaluated_per_call.surql` |

Each slot also carries its evaluation frame: `Caller`, `Permission` (reads unchecked, may not
write), `MutablePermission`, or `Computed` (reads checked as the definer, may not write), with an
optional `AuthLimit` (`expr/src/expr/logical/mod.rs:196-278`). The frame belongs to the slot, so a
rule that moves an expression between slots moves it between frames and must not.

Each slot carries its effect summary: `pure`, `read_only`, `may_raise`, `may_signal`, and whether
it materialises records (a dereference or traversal runs computed fields and permissions).
Projection may run column-major only when at most one column has effects and the others are total
(`exec/logical/rules/mod.rs:797-897`).

---

## 4. Ordering

A statement's rows are in a specified order only where the statement asks for one:

- **`ORDER BY`** specifies the order of its keys. Rows that tie on every key are in no specified
  order.
- **A record-id range source** (`t:1..10`) delivers its records in record-id order, as the
  documentation states. A range read delivers that order at no cost.
- **Everything else is unspecified.** Without `ORDER BY`, a table scan, an index scan, a union, a
  bitmap, `FROM a, b` and a join may return rows in any order, and the order may change from one
  execution to the next as the chosen plan changes. `LIMIT` and `START` without `ORDER BY` select
  an unspecified subset. The documentation states no default order for a table scan.

What the planner does with that:

- Every node declares its **delivered ordering**: key order of a table or range scan, index order,
  doc-id order for a bitmap, branch order for a union, written order for `FROM a, b` and for write
  targets, the left input's order for a join, sort-key order then arrival for a sort, group-key
  order for a `GROUP BY`. A delivered ordering is what lets a rule remove a `Sort` or reverse a
  scan, and what a range source must keep.
- A `Sort` whose input already delivers its keys in the same comparison is removed; one whose input
  delivers them reversed is replaced by reversing the input where the input can reverse (a scan, a
  range, an index scan). The comparison must match exactly: index key encoding against value
  ordering, `NONE`/`NULL` placement, `COLLATE` and `NUMERIC`, and an array field's fan-out to one
  index entry per element.
- Only a record-id range source and a `Sort` constrain which plan may be chosen. Otherwise a rule
  may replace a node with one that delivers any ordering.
- Fan-out (§8) under an `ORDER BY` or a range source must deliver extents in key order.

The language harness compares results in order and has no unordered mode, and tests pin today's scan
order (for example `language/primitive/record_id/natural_order.surql`,
`language/statements/select/start_limit_multiple_whats.surql`). Before any rule delivers an order
different from today's for a statement §4 leaves unordered, the harness needs an unordered
comparison and those tests need an `ORDER BY` or that mode. Until then the defaults of §7.2 choose
as today's fixed scores do, so no answer the suite checks changes.

---

## 5. Errors and access-path semantics

### 5.1 What aborts, what is empty

| outcome | cases |
|---|---|
| aborts the statement | any raise in a `WHERE`, projection, sort key, `TIMEOUT`, `VERSION` or source; `TbNotFound`; `NoIndexFoundForMatch`; `SingleOnlyOutput`; `QueryTimedout`; malformed KNN shapes; any failure on a write subject |
| no row or `NONE` | table permission false; a denied or missing dereferenced record; `record::exists` on a missing table |
| skipped | a wrong-kind value inside a streaming aggregate, when the group folded a usable value (`ControlFlow::is_ignorable`, `expr/src/expr/error.rs:245`), pinned by `repro/group_aggregate_wrong_kind_*` |

A rule that turns a plan into empty rows (contradiction, `LIMIT 0`, `WHERE false`) keeps the
source's existence and permission obligations: the result is empty rows from a resolved source, not
no plan.

When work is split across threads, the error a caller sees is the one the sequential order would
reach first: the lowest-index failure wins, as `exec/fan_out.rs` already does.

### 5.2 Access paths with semantics

| operator | without the index | consequence for the optimizer |
|---|---|---|
| `@@` | a table source raises `NoIndexFoundForMatch`; under `WITH NOINDEX` it is judged per row through the full-text executor; a record-id source answers `false` | the index binding (`MatchesScope`) is a plan property, established at mapping, not a choice |
| KNN `<\|k,EF\|>` | membership is `false` on every row | HNSW binding is established at mapping |
| KNN `<\|k\|>` over KTree, KNN not at top of an AND chain | error | mapping raises |
| count paths | never under `VERSION` | a versioned scope forbids them |
| any index | never over a field whose select permission is not `FULL` | eligibility reads field permissions, which are a dependency |
| `WITH INDEX` / `WITH NOINDEX` | restrict which paths are eligible, never the rows | constraints on the access-path rule |

---

## 6. Catalog inputs and the cache key

### 6.1 What the catalog provides

All per-table facts are in the `surrealdb/catalog` crate and read through `TableProvider`
(`catalog/src/providers.rs`), with per-transaction memoisation.

| fact | where | used for |
|---|---|---|
| schemafull | `TableDefinition.schemafull` (`catalog/src/table.rs:77`) | closed schema: an undefined field is absent, so a projection over it prunes and a typed slot layout is possible |
| table type, relation, lightweight, enforced | `TableDefinition.table_type` | scan kind; `CREATE`/`RELATE` checks |
| table permissions | `TableDefinition.permissions`; `Full` by `matches!` | whether a `PermissionFilter` exists; index and graph pushdown gates |
| view definition | `TableDefinition.view` | write observers |
| fields: kind, flexible, readonly, value, assert, computed, default, field permissions, reference | `FieldDefinition` (`catalog/src/schema/field.rs:307`) | kind-gated rules (§6.3); computed layers; field guards; write stages; cascades |
| indexes: kind, columns, count condition | `IndexDefinition` (`catalog/src/schema/index.rs:696`) | access paths; count paths; `supports_order()` for Idx/Uniq only |
| index online state | `filter_online_indexes` (`surrealdb/datastore/src/index_state.rs:244`) | eligibility |
| events, live queries | `all_tb_events`, `all_tb_lives` | write observers, footprint |
| incoming references | `table_may_have_incoming_references` (`surrealdb/datastore/src/tx.rs:739`) | delete cascades, footprint |
| functions, modules, params | `get_db_function`, `get_db_module`, `get_db_param` | access mode, inlining, parameter folding |

**Schemafull is not a kind guarantee.** A field with no `TYPE`, or a `FLEXIBLE` one, holds any
value in a schemafull table. Rules that need a kind read `field_kind`, not `schemafull`.

### 6.2 Versions

A per-table `CatalogVersion(Uuid)` under `!cv{tb}` is bumped by every table, field and index
definition change, and by events and views through `put_tb` (`surrealdb/datastore/src/tx.rs:2201`,
`values/catalog_version.rs`). A node-wide `TableSnapshot` cache is already keyed on it
(`surrealdb/datastore/src/cache/ds/catalog.rs`). Function snapshots are content-addressed.

The cache key is therefore: the statement, the permission regime, and for each dependency its
version or content hash. What has no version today, and what the key needs from it:

| dependency | versioned today | requirement |
|---|---|---|
| table, fields, indexes, events, views | yes, the table's `CatalogVersion` | — |
| index online state (`!bs`) | no | key it, or re-check eligibility when the plan is fetched; unkeyed, a plan keeps a table scan after an index comes online (correct, slow) |
| live queries | no, only `cache_lives_ts` | key it for write plans that notify |
| incoming references (a `REFERENCE` on table A targets B) | A's version moves, B's does not | record A as a dependency of B's delete plans |
| `DEFINE PARAM` | no | fold a param only if its value can be keyed; otherwise leave it a runtime read |
| functions | content hash | — |
| namespace, database | no | key by id; a database with `strict` changes semantics and needs a version |

### 6.3 Rules the catalog enables

These are the rules `LOGICAL_RULES.md` classifies as physical, now expressible: sargability,
function inversion, pattern-to-range and `IN` → range (each gated on a declared `field_kind`),
data-clause subquery hoisting and footprint-checked hoisting generally (gated on the write
footprint of §8.3), `SET a = a` (gated on the field's select permission), gate subsumption, event
pruning, `RETURN NONE` narrowing of computed fields, and the index-backed write `WHERE`.

---

## 7. Statistics

### 7.1 What exists

| statistic | exists | cost | evidence |
|---|---|---|---|
| table row count, no index | no; `txn.count` scans the prefix | O(n) | `surrealdb/kvs/src/api.rs:899` |
| row count with `DEFINE INDEX … COUNT` | exact, transactional deltas compacted in the background | one compacted key plus a key per commit since compaction | `surrealdb/idx/src/count.rs` |
| full-text document count and total length | exact, transactional | a point read plus deltas | `surrealdb/idx/src/ft/fulltext.rs:1139-1190` |
| per-term document frequency | implicit in the term bitmaps | paid at query time | `fulltext.rs:603` |
| HNSW, DiskANN | allocator state only; `next_element_id` bounds elements ever inserted | point read | `surrealdb/datastore/src/values/hnsw.rs:46` |
| index cardinality, distinct keys, histograms, sizes | no | — | — |

Access-path choice today uses fixed scores (`exec/index/analysis.rs:2130-2190`), not statistics.

### 7.2 Requirements

- **The optimizer runs correctly with no statistics.** Every rule has a default that is sound, and
  the fixed scores are that default.
- **Statistics are read through one interface** that answers "unknown" as a first-class value, so a
  rule never confuses an absent statistic with zero.
- **An exact statistic is never required.** The count index is exact, but a plan must not depend on
  exactness, because `COUNT` indexes are optional and their read cost grows with the uncompacted
  backlog.
- **What to add, cheapest first:**
  1. An approximate row count per table, maintained like the count index (a per-transaction delta
     written at commit) but always on and never read for an answer. It drives build side, fan-out
     width, and whether to fan out at all.
  2. Per-index entry and distinct-key estimates, updated the same way, for index versus scan.
  3. Sampled histograms, refreshed in the background, for range selectivity.
  Each is a persisted format and brings the upgrade-test obligations in `CLAUDE.md`.
- **A plan records the estimates it used**, so that a cached plan whose estimates have drifted by
  more than a threshold is re-optimized. This is a performance trigger, never an invalidation
  (§1, law 1).

---

## 8. Fan-out

The VM fans out to threads. What the transaction layer allows today:

- `Transaction` is `Send + Sync` and shared as `Arc` (`exec/context.rs`).
- Concurrent reads on one transaction share a read lock on mem and surrealkv. rocksdb holds a mutex
  across every operation and tikv takes a write lock even for `get`, so one transaction serialises
  there (`surrealdb/kvs-rocksdb/src/lib.rs:1070`, `surrealdb/kvs-tikv/src/lib.rs:920`).
- No backend offers reads of one transaction's snapshot from several tasks. Such an interface
  must refuse a transaction that holds buffered writes or tracks a read set for commit-time
  conflict detection, since reads outside the transaction see neither.
- Savepoints are a LIFO stack with no handle and must be driven from one task; two concurrent
  writers roll back each other's work (ADR 0001, R3).

### 8.1 What the plan marks

| property | meaning | set by |
|---|---|---|
| partitionable source | a key range that can be split into disjoint extents | map: table, range and index scans |
| read-only leg | the subtree beneath the split reads only | effect summary |
| order requirement | concat extents in key order, merge by sort key, or unordered | §4 |
| fold | concat, sorted merge, top-k merge, partial monoid (`count`, `sum`, `min`, `max`), hash aggregate | the consumer |
| width | number of extents | optimize, from the row-count estimate; 1 when unknown |
| placement | `Serial`, or `Morsels` with an input, a combine and whether the paging is replayed, per pipeline; stored on the pipeline's source node | `exec/vm/place.rs`, the last step of optimize, through `Policy::plan` over the logical legality (`PLAN_LAYERS.md`) |

### 8.2 Requirements

- **Only read-only legs fan out.** A leg containing a write, a writing function, or an effect whose
  count is observable runs on one task in input order. A mutation nested anywhere in a `SELECT`
  makes the whole plan read-write (ADR 0001, Decision 4). Every pipeline of a statement that writes
  is placed serial, and so is every read nested in it: a plan nested in a statement, and a unit
  that lowering compiles into the statement's program, is placed in the scope of the statements
  around it. A unit a running program loads from the compile cache is placed in a scope of its
  own.
- **Parallel reads need a snapshot-read interface.** Without one, fan-out runs extents
  concurrently on one transaction and serialises on rocksdb and tikv. Implementing it for
  rocksdb and surrealkv is a prerequisite for fan-out to speed anything up.
- **Order is preserved where it is specified** (§4): extents are delivered in key order under an
  `ORDER BY` the scan satisfies or a record-id range source, and in any order otherwise.
- **Errors are deterministic** (§5.1): the lowest-index failure wins.
- **Evaluation counts survive the split.** A leg evaluates each row's slots exactly as the
  sequential plan would, and per-statement slots are evaluated once, before the split, and passed
  to every leg.
- **Width is a statistics choice** and therefore never changes an answer.

### 8.3 The write footprint

A write's footprint is its target tables plus the transitive closure through the catalog: views
over them, events on them and whatever those events write, reference cascades and edge cascades.
It is physical (ADR 0003) and is computed at mapping from the dependencies already recorded. A read
may be hoisted out of, reordered past, or run concurrently with a write only if its reads are
disjoint from that footprint.

---

## 9. Lowering to the VM

### 9.1 The model

A statement lowers to fused per-row loops between pipeline breakers (`Sort`, `Aggregate`, join
build, top-k). A scan is a cursor (`SCAN_OPEN`, `NEXT`); filters are jumps; pre-emption is a fuel
check on the back-edge; the program is driven as a stream by an async host. Prefetch is a hint to
the storage layer below the transaction, and reads still go through read-your-own-writes, so it
needs no plan property.

### 9.2 Where the VM must hand back to the host

A VM thread does not await. Every per-row evaluation that awaits (`Clause::evaluate`
is async only, `exec/clause.rs`) becomes a segment cut: the VM yields, the host
does the work, the VM resumes.

| awaits today | examples | evidence |
|---|---|---|
| storage | cursor advance, record fetch and dereference, graph and reference lookups | `exec/operators/fetch.rs:542-575`, `exec/parts/lookup.rs` |
| a nested plan | scalar subqueries, user functions, closures, event bodies | `exec/vm/subquery.rs`, `exec/vm/function.rs` |
| catalog | function and module lookup | `exec/vm/function.rs`, `function/module.rs` |
| network, CPU offload | `http::*`, `file::*`, password hashing, ML models, scripting | `exec/function/macros.rs`, `function/model.rs:152` |

Permissions and computed fields reach storage through dereference, so any row can reach a cut.

### 9.3 Requirements on the VM

- **An `EVAL` fallback** evaluates an expression the VM does not compile natively through the
  existing evaluator, as a segment cut. Coverage is then limited by plan nodes, not expressions.
- **Per-row allocation is avoided at the representation**: field reads borrow from the record's
  encoded bytes. No step clones its operands, no row constructs a VM, and an execution does not
  clone the program's constants.
- **Value conversions are lossless.** Strings and bytes never become `NONE`, UUIDs are never
  truncated, and `U64`/`U128`/`I128` never narrow through an `as` cast.
- **The clock follows ADR 0006.** A `WHERE` reads a statement-start slot, not the clock on every
  execution. Fuel accounting does not read the clock per instruction.
- **Parameters are parameters.** An unscoped `$param` never compiles as a field read.
- **Subqueries are real.** A subquery's value is computed, never a placeholder slot.
- **The VM answers as the expectations say**, and a statement it declines fails its test: there is
  no second executor to answer instead.

---

### 9.4 Pipelines and sinks

The physical plan decomposes into **pipelines**: chains of nodes that run in
one loop over the rows they pass along, each ending at a **sink** — the input
side of a node that holds rows (an aggregate, a top-k, a hash-join build, the
statement's output). A sink's output side is the source of the next pipeline.

- **The plan decides; lowering emits.** Which nodes share a pipeline and how each sink is
  implemented are physical decisions, made by rules (hash or streaming aggregate, top-k or full
  sort, partial and final aggregate). Lowering emits one loop per pipeline with the sink's input
  side as instructions inside it, and chooses nothing.
- **A fused sink keeps the node's semantics.** An aggregate accumulated in the scan's loop answers
  what the aggregate operator answers: its evaluation classes (§3) — a key and a first argument per
  row, an extra argument per group — its group order, its answer over no rows, and its treatment of
  values of an unexpected kind. A typed accumulator is checked against the existing accumulator for
  the same function over generated inputs of every kind, and a kind it does not answer is passed to
  the existing accumulator from inside the loop.
- **Partial and final.** An aggregate over a fanned-out pipeline splits into a partial sink in each
  leg and a final merge at the coordinator, by the fold classes of §8.1: count, sum, min and max
  merge their partials; a function that has no merge runs final-only, over rows the legs send.

### 9.5 Scheduling and preemption

- **A VM's state is explicit** — the instruction it resumes at, its registers, its cursor
  position — so switching between VMs is a return to the host's scheduler, and resuming is
  continuing at that state.
- **Where a program yields.** A program run on decoded rows yields on fuel at a backward jump. A
  row program borrows its row and cannot yield inside one; it yields between the chunks the storage
  cursor hands over, and the scheduler bounds a slice by the chunk size it asks for.
- **Fairness.** VMs share a node through a run queue with a fuel quota per slice, per tenant;
  foreground statements and triggered programs are scheduled alike.
- **Morsels.** A partitionable scan (§8.1) is divided into morsels — key extents of a size the
  planner chooses from statistics — that workers take from a shared queue, each running the
  pipeline's program with sinks local to the worker, merged when the pipeline ends. Morsels carry
  their sequence number: where §4 specifies an order, the consumer receives them in sequence.
  Whatever the order, the failure of the lowest-numbered morsel is the one reported (§5.1).
- **Width is elastic.** How many workers take morsels from a pipeline's queue is the scheduler's
  choice, made while the pipeline runs and changed whenever it likes: fewer when the node is
  saturated, when other tenants' VMs are waiting, or when legs contend — on a transaction's
  serialised backend, on a storage range, on a remote shard — more when the queue is long and cores
  are idle. Nothing about the answer depends on it: every morsel runs the same program into
  worker-local sinks, results are ordered by morsel sequence, and a worker added or withdrawn
  between morsels changes only when a morsel runs, not what it produces.
- **A leg on a Worker.** A Worker isolate runs one thread under a CPU-time limit per invocation, so
  a Worker takes one morsel per invocation and yields on fuel. A leg approaching its limit
  checkpoints — its resume point, registers and cursor position, in the format of a triggered
  program's checkpoint (`RFC_DURABLE_BYTECODE_RUNTIME.md` §4.2) — and returns the checkpoint in
  place of a result; the coordinator resumes it in a new invocation. Preempting a leg, resuming a
  crashed triggered program and waking a waiting one are the same operation.

### 9.6 Memoized subexpressions

A subexpression of a predicate or a projection may be evaluated once for a
statement's execution, the first time a row reaches it, and reused for every
later row, only when all of these hold:

- **It is row-free.** It reads no field of the row, no document, and no row-scoped parameter
  (`$this`, `$value`, `$parent`, `$before`, `$after`).
- **It is effect-free** (`is_effect_free`): no write, no clock, no entropy, no external call.
- **Nothing the statement can do writes what it reads.** Its read footprint is disjoint from the
  statement's static write footprint — every write on every path of the statement's control
  graph, however unlikely the path, together with everything those writes cause: events, views,
  reference and edge cascades, and the bodies of the functions it calls (§8.3). The check is
  static, against what the statement can write, never against what one run happened to write. A
  statement whose access mode is read-only has an empty write footprint.

It is evaluated lazily — when first reached, not when the statement starts — so
that a subexpression that raises raises on the first row that reaches it, and one
an earlier operand of `AND` or `OR` skips is skipped. It is evaluated in the frame
of the expression it belongs to: a permission predicate's memoized part in the
permission frame, under the definer's authority. The memo belongs to one
execution and is never carried across executions, callers or statements.

## 10. Divergences and gaps that affect the plan

Pinned behaviours, where the test's expectation is the reference:
a value source materialising past `LIMIT`; a denied field projected under its own name; denied
computed fields under `WHERE false`/`LIMIT 0`; `ONLY` over an unbounded array; aggregate typing and
per-row versus per-collection projection; fused graph-hop clauses; `FETCH` depth; `SPLIT` over a
record link; `ORDER BY` a field not selected (all in `language-tests/tests/reproductions/`).

Not pinned, and needed before a rule depends on them:

| behaviour | why the plan cares |
|---|---|
| an effectful projected column under `ORDER BY … LIMIT` | whether a projected `sequence::nextval()` is counted for every row sorted or only for the rows kept is not pinned, and `LOGICAL_RULES.md`'s limit-pushdown gate assumes it does not matter |
| a dereference inside a permission predicate on a missing table | `fetch_record_no_perms` answers `NONE`; `fetch_record` raises |
| written subject order of `UPDATE t:2, t:1` | write sources must preserve order |
| a unique-value swap inside one statement | eager unique checks make subject order decide it |
| a before-image prefetched ahead of an event that updates it | write sources buffer rows (`exec/buffer.rs:111`); an event on an earlier subject can make a later buffered image stale |
| live and changefeed payloads | observers the plan must keep |
| `sequence::nextval` across rollback | an effect whose count is observable |

The `READS_DATA` documentation in `surrealdb/expr/src/expr/logical/effects.rs:62` describes reads
as snapshot-stable within a statement, which does not hold across the subjects of a write (§2.2).

---

## 11. Open decisions

1. **How a VM worker reaches storage.** Either a worker is an async task that runs the synchronous
   loop between cuts, or a worker is a thread that hands cuts to a host runtime. Both satisfy §9.2;
   they differ in fairness and thread-pool shape.
2. **The statistics format** of §7.2: whether the approximate row count shares the count index's
   delta mechanism, and how drift is measured.
3. **Index online state in the key** (§6.2): keyed, or re-checked on fetch.
4. **`EXPLAIN` from the plan.** Rendering the physical plan changes `EXPLAIN` output, which tests
   pin; the new path needs its own expectation or a shared format.
