# The VM as a fan-out leaf

**Status:** contract for the leaf profile, and the inventory the host-boundary extraction works
from. The classification is code: `surrealdb/core/src/exec/vm/leaf.rs`. This document states what
it means and what each class costs to serve away from the engine.

## 1. What a leaf is

A leaf runs one plain-data `vm::Program` — a program `assert_plain!` admits: no handle, no
planned expression, no operator — read-only, over a snapshot of storage, in a process that holds
no datastore: a Cloudflare Worker reading a snapshot KV, for example. It runs scans, filters,
projections, aggregates, sorts and top-k, joins, pure builtins, and closures whose bodies are
units the program links.

A leaf never stages a write, defines or reports the catalog, notifies a live subscription, runs an
embedded script, a module or a model, reaches the network or a bucket, waits, or runs a body the
program does not hold (`eval::*`, a stored API endpoint). Its host serves the program through a
small set of narrow interfaces (§3), not through the engine's `ExecutionContext`.

## 2. The classes

Every instruction, source and host step has exactly one class:

| class | meaning |
|---|---|
| `Leaf` | The interpreter answers it from the program, the row and the running state alone, or the host does so from the program and the row. |
| `Kernel(seams)` | A leaf needs it. The host answers it today through code the operator tree owns, called with the engine's execution or evaluation context; a leaf host serves it through the listed seams. |
| `NonLeaf(bar)` | A leaf never runs it. |
| `ByOperand` | A host step whose class is that of the instruction or callee it services: `Call`, `Method`, `Block`, `Store`, `External`. |

A program is **leaf-clean** when nothing in it, or in any unit it holds, is `NonLeaf`: a leaf host
that serves the seams runs it. It is **host-free** when everything in it is `Leaf`: it runs on the
interpreter and the pure builtins alone.

### How the table is kept whole

- `leaf::step` matches every `interp::Step` variant, `leaf::instr` every `Instr` variant,
  `leaf::source` every `Source` variant, and the block, store and write matches every
  `BlockInstr`, `StoreInstr` and `WriteInstr` variant, with no wildcard arm. A new variant does not
  compile until it is classified.
- An instruction the interpreter answers without yielding is `Leaf`. One that yields takes the
  class of the step it yields; `leaf::instr` states it by calling `leaf::step`, so a step is
  classified once.
- The write sub-families (`DataInstr`, `SchemaInstr`, `GateInstr`, `ObserveInstr`,
  `StorageInstr`) are classified as families: every one of their instructions is part of a
  record's write.
- A builtin's class comes from its declared `EffectSet` (`leaf::builtin`): `OPAQUE_BODY` bars it as
  opaque; `WRITES_DATA` or `ADVANCES_STATE` as a write; `WRITES_EXTERNAL` or `READS_EXTERNAL` as
  network; `BLOCKING` as a wait. Otherwise a pure synchronous builtin is `Leaf` — it is invoked on
  its arguments alone — and any other is `Kernel`, through the seam its read effects name, plus
  `TreeCode` because `ScalarFunction::invoke_async` takes an `EvalContext`. A method is the most
  constrained of the implementations its receivers resolve to. A projection function is
  `Kernel(TreeCode)`.
- `leaf::SERVICES` classifies every method of `vm::services::Services`; a test parses the trait and
  fails until a new method has a row.

## 3. The seams

A seam is a narrow interface a leaf host implements over what it has: a snapshot, a catalog copy,
a principal.

| seam | what it serves | today |
|---|---|---|
| `Storage` | records by id and range, B-tree, bitmap, full-text and ANN index reads, counts, the adjacency, as of the snapshot | `Services` (16 of its 26 methods), plus direct `kvs::Transaction` use in `host.rs`, `nested.rs`, `graph.rs` |
| `Catalog` | table, field, function and parameter definitions | `Services::read_table`, `read_function`, `read_db_param`; `TableProvider` through the transaction |
| `Permissions` | the principal, table and field permissions of a record read, the capabilities that admit a function | `exec::permission`, `EvalContext::check_allowed_function`, `ctx.capabilities()` |
| `Clock` | time, the deadline, cancellation | `check_cancelled`, the context's deadline |
| `Params` | session and frame parameters, the frames a block saves and restores | `ExecutionContext::value`, frames held in the running state |
| `Link` | the unit of a closure or of a table only the data names | `closure::relink`, `dispatch::Deref`; both fault to the compiler (`fault::serve`) when no table holds the unit |
| `TreeCode` | not an interface: evaluator code of the operator tree that a host step calls with an `EvalContext`. Each use is ported into instructions or reduced to the seams above. | `physical_expr`, `parts`, `operators` |

A leaf cannot compile, so a leaf host serves `Link` only from units the program ships: a program
whose closures or dispatched reads need a unit it does not hold fails on the leaf where the engine
would fault.

## 4. Inventory

Counts are variants. "Kernel" lists the code each item reaches today and the seam that replaces
it.

### Totals

| family | variants | Leaf | Kernel | NonLeaf | ByOperand |
|---|---:|---:|---:|---:|---:|
| `interp::Step` | 27 | 4 | 17 | 1 | 5 |
| `Instr` (top level) | 98 | 68 | 23 | 2 | 5 (`Call`, `Method`, `Block`, `Store`, `Write`) |
| `BlockInstr` | 12 | 6 | 6 | 1 | — |
| `StoreInstr` | 65 | 0 | 0 | 65 | — |
| `WriteInstr` (+ 59 in sub-families) | 31 | 0 | 0 | 31 | — |
| `Source` | 20 | 7 | 10 | 3 | — |
| `Services` methods | 26 | 0 | 19 | 7 | — |

`BlockInstr::Bind` is `Leaf` without a frame binding and `Kernel(Params)` with one, so it counts
in both columns. `Instr`'s `Leaf` column is the 66 instructions the interpreter answers alone plus
`LandRow` and `Signal`, whose steps the host takes from the program and the row.

### Host steps

| step | class | reaches today | replaced by |
|---|---|---|---|
| `Next`, `Stop`, `LandRow`, `Signal` | Leaf | the running state | — |
| `FetchRow` | Kernel | `operators::fetch::fetch_record` | Storage, Permissions |
| `Fetch` (`FETCH`) | Kernel | `operators::fetch::fetch_fields_with`, `FetchWalk`/`FetchIndex` | Storage, Permissions; the walk ported |
| `Access` (field over a record link, array of links, geometry) | Kernel | `parts::field::evaluate_field` | Storage, Permissions; ported |
| `Each` (`[*]` over links) | Kernel | `parts::array_ops::evaluate_all` | Storage, Permissions; ported |
| `Lookup` (graph and reference hops) | Kernel | `vm::graph`: `idx::adjacency`, `operators::scan::{graph_keys, ref_key_range, payload_fields, common}`, `permission::validate_record_user_access`, `kvs::Transaction` | Storage, Permissions |
| `Degree` | Kernel | `physical_expr::graph_degree::DegreeCounter` | Storage |
| `Recurse` (`@` recursion, repeat) | Kernel | `operators::recursion::{DepthBody, RepeatBody, evaluate_repeat_recurse}`, `physical_expr::RecursionCtx` | Storage, Permissions; ported |
| `Destructure` | Kernel | `operators::fetch` through `host::destructure` | Storage, Permissions |
| `Param` | Kernel | `vm::function::{frame_param, stored_param}`, `Services::read_db_param` | Params, Catalog |
| `CallAllowed` | Kernel | `EvalContext::check_allowed_function` | Permissions |
| `Call` | by builtin | `host::call_builtin`: registry, `experimental_target`, `ScalarFunction::invoke[_async]` | Leaf when pure; otherwise the seams of its effects, and `TreeCode` for the `EvalContext` |
| `Method` | by method | `host::call_method`: `MethodDescriptor::resolve`, then as `Call` | as `Call` |
| `WriteProjection` | Kernel | `ProjectionFunction::invoke_async`, `operators::projection_writes` | TreeCode |
| `IndexFn` | Kernel | `IndexFunction::invoke_async` with `IndexContext` | Storage; ported |
| `Matches` | Kernel | `physical_expr::MatchesEval` | Storage; ported |
| `Subquery` | Kernel | `host::run_subprogram`: `physical_expr::subquery_context`, `trampoline::nest` | Params |
| `Function` (`fn::` enter, bind, return, call, dive) | Kernel | `vm::function`: `Services::read_function`, `physical_expr::{EnteredCall, function::in_limited, function::helpers}`, `operators::permission_frame`, `dbs::capabilities` | Catalog, Permissions |
| `Closure` (make, invoke) | Kernel | `physical_expr::function::CompiledClosure::planned`, `closure::invoke_linked`, `helpers::invoke_uncompiled_closure` | Link; the uncompiled fallback ported |
| `NeedUnit` | Kernel | `closure::relink` → `fault::serve` | Link |
| `Block` | by block instruction | `vm::block::step`, `vm::nested::statement` | below |
| `Store` | by store instruction | `vm::store::step`, `vm::backfill::step` | — (all NonLeaf) |
| `Write` | NonLeaf (write) | `vm::write::steps` | — |
| `External` | by callee | `vm::external` over `Services` script, model and module methods | — (all NonLeaf) |

### Block instructions

| instruction | class |
|---|---|
| `RunEmbedded`, `CoerceKind`, `Return`, `IterStart`, `IterNext`, `Bind` without a frame | Leaf |
| `Bind` into the frame, `Save`, `Restore`, `Nested` | Kernel (Params): the frames are `ExecutionContext` clones; `Nested` runs `vm::nested`, whose sources reach `operators::scan::pipeline::kv_scan_stream`, `operators::fetch::*`, `exec::permission`, `pre_decode_filter` |
| `Interrupt` | Kernel (Clock) |
| `RunAnalyzed` | Kernel (Clock, TreeCode): `operators::render_analyze_text` |
| `Sleep` | NonLeaf (sleep) |

### Store and write instructions

Every `StoreInstr` is part of a schema statement's, an `INFO`'s or a view backfill's work over the
catalog and the process: 52 are barred as schema work, 4 as a write (`EvaluateOver`, the three
`Backfill*`), 4 as speculation (`NextId`, `NewUuid`, `RealRunOnly`, `JumpIfSpeculating`), 2 as
modules, 1 each as live (`KillOnCommit`), model (`DeleteModelArtifact`) and network
(`ConnectBucket`). Every `WriteInstr` is a record's write; `Observe` is barred as live.

### Sources

| source | class | reaches today |
|---|---|---|
| `Sorted`, `Groups`, `Split`, `Once`, `Union`, `Nested`, `Probe` | Leaf | the running state, the program's groups |
| `Scan`, `Record`, `LightweightEdges` | Kernel (Storage) | `host::run_storage_source`: `operators::scan::pipeline::kv_scan_stream`, `scan::common::resolve_version_stamp`, `scan::record_key_range`, `topk_pushdown::TopKThresholdProbe`, `kvs::lightweight::lightweight_relation`; `Services::scan_lightweight_edges` |
| `Index`, `IndexUnion`, `Bitmap`, `FullText`, `Knn` | Kernel (Storage) | `Services::{index_rids, evaluate_bitmap, resolve_doc_ids, full_text_candidates, knn_candidates, read_records}` over `operators::scan::{bitmap, fulltext, knn}` and `exec::index::iterator::btree` |
| `Count`, `Values` | Kernel (Storage, Permissions) | `vm::count`: `permission::should_check_perms`, `Services::count_*`; a value's record ids read through `fetch_raw_record` or the dispatch |
| `WriteTargets`, `WriteInserts`, `WriteEdges` | NonLeaf (write) | — |

### Program-level

| property | class |
|---|---|
| `write`, a group's `write`, `events`, `stage_set` | NonLeaf (write) |
| `reservations`, `implicit_tables` | NonLeaf (speculation) |
| `reach` (reads by a table only the data names) | Kernel (Link, Catalog) |
| a `TIMEOUT` | Kernel (Clock) |
| a `VERSION` | Kernel (Storage) |
| permission frames | Kernel (Permissions) |

### Services

| methods | class |
|---|---|
| `index_rids`, `next_index_rids`, `evaluate_bitmap`, `resolve_doc_ids`, `full_text_candidates`, `knn_candidates`, `scan_lightweight_edges`, `read_records`, `read_record`, `table_range`, `record_range`, `count_keys`, `record_exists`, `count_index_deltas`, `count_btree_keys`, `bitmap_cardinality` | Kernel (Storage) |
| `read_table`, `read_function` | Kernel (Catalog) |
| `read_db_param` | Kernel (Params, Catalog) |
| `check_script`, `call_script_dynamic`, `read_module`, `module_export`, `call_module` | NonLeaf (script) |
| `read_model`, `compute_model` | NonLeaf (model) |

## 5. The shared kernel

What the leaf path imports from outside `exec::vm`, grouped by what the extraction does with it.
Sizes are the lines of the providing module or file, test modules excluded; a module the leaf
reaches only part of is an upper bound.

### The context the host passes everywhere

The running state and every `Kernel` host step are written against the engine's contexts, not
against the seams:

- `interp::State` holds `ExecutionContext`s: `frames` (permission frames), `frame` and `saved`
  (a block's frames), `calls[..].limited` (`physical_expr::EnteredCall`, a function's frame). The
  state also holds `physical_expr::{MatchesEval, ComputingRecord, RecursionCtx}`,
  `exec::function::{KnnContext, MethodDescriptor}` and the `DegreeCounter`s.
- `host::frame_eval` builds a `physical_expr::EvalContext` (`physical_expr/mod.rs`, 661 lines)
  over an `ExecutionContext` (`exec/context.rs`, 1289 lines) for 21 of the host's step arms.
  `host.rs` names `ExecutionContext` 51 times, `services.rs` reads the transaction through it 8
  times, `nested.rs` 11, `dispatch.rs` 13, `region.rs` 13.

Replacing these with a leaf host type that implements the seams is the first cut of the
extraction: every other item below is reached through them.

### Ordered by size

| # | dependency | lines | reached from | extraction |
|---:|---|---:|---|---|
| 1 | `exec::operators::scan` — `pipeline::{kv_scan_stream, decode_record}` (2304), `bitmap` (1452), `knn` (1107), `index_count` (732), `resolved::EnforcementMode` (579), `fulltext` (385), `graph_keys` (349), `common` (316), `lightweight` (286), `record_key_range`, `ref_key_range`, `payload_fields`, `MergeMode` | 16462 module; ~7500 reached | `host`, `services`, `nested`, `graph`, `row` | Storage: index structures move behind `Services` (already the interface for 16 methods); `kv_scan_stream` and `decode_record` are read through the snapshot directly |
| 2 | `exec::function` — registry, `ScalarFunction`, `AggregateFunction`, `MethodDescriptor`, `IndexFunction`, `KnnContext`, `IndexContext`, `MatchContext`, the builtins | 11898 | `interp`, `host`, `inline` | shared crate: pure builtins and aggregates run as they are; `invoke_async`'s `EvalContext` argument is the TreeCode seam of every non-pure builtin |
| 3 | `exec::pre_decode_filter` (6456) and `exec::object_extract` (1379) | 7835 | `row`, `nested`, `program` | shared crate: pure wire codec, no context |
| 4 | `exec::permission` — `validate_record_user_access`, `should_check_perms` | 2855 | `host`, `services`, `nested`, `graph`, `count`, `dispatch`, `fault` | Permissions |
| 5 | `exec::operators::recursion` — `DepthBody`, `RepeatBody`, `evaluate_repeat_recurse` | 866 + 2954 | `host` | TreeCode: port to instructions over units, or run behind Storage and Permissions |
| 6 | `exec::physical_expr::function` — `CompiledClosure` (1077), `user_defined`/`in_limited` (805), `helpers` (192), `bind_closure_args`, script/model/module entry points | 3193 | `host`, `interp`, `function`, `closure`, `services` | Link and Catalog for closures and `fn::`; the script, model and module entry points stay with the engine |
| 7 | `exec::operators` (root) — `GroupMap`, `GroupState`, `compare_values`, `first_value_needed`, `AggregateId`, `record_of`, `group_key_field_name`, `aggregate_field_name`, `split_row`, `set_nested_value`, `projection_writes`, `SortDirection`, `check_cancelled`, `limit::paging_count`, `render_analyze_text`, `permission_frame` | 226 + `aggregate` 2463 + `limit` 244 | `interp`, `host`, `region` | shared crate: pure code; `check_cancelled` is Clock, `permission_frame` Permissions |
| 8 | `exec::physical_expr` (root) — `EvalContext`, `EnteredCall`, `ComputingRecord`, `RecursionCtx`, `subquery_context`, `MatchesEval`/`MatchProbe` (866), `graph_degree::DegreeCounter` (297), `ExecutorTables` | 661 + 866 + 297 + 151 | everywhere | replaced by the leaf host type; `MatchesEval` and `DegreeCounter` behind Storage |
| 9 | `exec::operators::fetch` — `fetch_record`, `fetch_raw_record`, `fetch_fields_with`, `process_fetched_record`, `batch_fetch_in_place`, `fetch_record_no_perms`, `require_table`, `FetchWalk`, `FetchIndex`, `WalkStep` | 853 | `host`, `services`, `nested`, `dispatch`, `fault` | Storage and Permissions; the computed fields a fetch applies are TreeCode |
| 10 | `exec::topk_pushdown` — `TopKThresholdCell`, `TopKThresholdProbe` | 706 | `interp`, `host`, `program` | shared crate: pure |
| 11 | `exec::parts` — `field::evaluate_field` (482), `array_ops::{evaluate_all, evaluate_flatten}` (468), `index::evaluate_index` (274), `doc::record_identity` (141) | 1365 | `host`, `program` | TreeCode: port the record-link cases to instructions over Storage and Permissions |
| 12 | `exec::trampoline` (401), `exec::compiles` (189), `exec::config` | 590 | `host`, `region`, `nested`, `graph`, `function`, `count`, `subroutine` | shared crate: runtime plumbing; `compiles::serving_fault` stays with the engine |
| 13 | `kvs` — `Transaction`, `Direction`, `CachePolicy`, `NORMAL_BATCH_SIZE`, `lightweight::lightweight_relation`, `DatastoreError` | — | `host`, `services`, `nested`, `graph`, `dispatch`, `closure` | Storage: the leaf reads the snapshot, not a transaction |
| 14 | `fnc::operate` | 91 | `scalar`, `program` | shared crate: pure |
| 15 | `idx::{adjacency, inline_cache}` | crate `surrealdb-idx` | `graph` | already a crate of its own; reached over Storage |
| 16 | `dbs::{Variables, capabilities::Error}`, `doc::Error::FieldCoerce`, `iam::{Auth, Action}`, `catalog` definitions | types | `host`, `function`, `interp`, `nested`, `services` | Params, Permissions and Catalog types the seams carry |

The planner side — `exec::planner`, `exec::logical`, `exec::catalog_deps`, `vm::fault`, `vm::units`,
`vm::speculate`, `vm::map`, `vm::lower*` — is the compiler, which a leaf does not hold. `vm::store`,
`vm::backfill`, `vm::write*`, `vm::live`, `vm::info` and `vm::external` serve only `NonLeaf` items.

## 6. The report

`SURREAL_VM_LEAF=report` makes the engine write one line to standard error for each statement it
plans onto the VM whose bound form holds no write, schema statement, `INFO`, `SLEEP`, `LET`,
`FOR`, block or `EXPLAIN`:

```
VM_LEAF kind=<select|return|expr> stmt=<hash> clean=<bool> host_free=<bool> blockers=[<bar>:<item>;…] kernel=[<item>;…] seams=[<seam>;…]
```

`stmt` hashes the statement's bound form, so repeated runs of one statement
name it once. The report reads the program; it changes nothing about how the statement runs.
Statements inside a block, and the bodies of functions lowered on their own, are not reported.

## 7. Measured status

Measured over the language suite, memory backend, with the VM offered every plan:
`SURREAL_VM_LEAF=report surrealql-test --color never run`, the `VM_LEAF` lines
grouped by `stmt` (16080 lines, 6048 distinct statements). A statement is counted once; an item
counts a statement once however often its program holds it.

### Leaf-clean fraction

| kind | statements | leaf-clean | | host-free | |
|---|---:|---:|---:|---:|---:|
| `SELECT` | 2575 | 2524 | 98.0% | 7 | 0.3% |
| `RETURN` | 842 | 771 | 91.6% | 215 | 25.5% |
| expression statement | 2631 | 2310 | 87.8% | 1199 | 45.6% |
| all | 6048 | 5605 | 92.7% | 1421 | 23.5% |

### Blockers

The write family is counted as one cause: a statement in an expression that writes, or a
function body that does, puts the whole write program in the unit table.

| rank | blocker | statements | its only blocker in |
|---:|---|---:|---:|
| 1 | Opaque: `api::invoke` | 142 | 142 |
| 2 | Write: a write the program holds (`Write::*`, a write source) | 69 | 68 |
| 3 | Schema: catalog work (`Store::*`) | 40 | 36 |
| 4 | Write: `sequence::nextval` | 36 | 36 |
| 5 | Script: embedded script | 28 | 28 |
| 6 | Opaque: `eval::surql` | 17 | 17 |
| 7 | Network: `file::get` | 15 | 15 |
| 8 | Network: `file::put` | 12 | 12 |
| 9 | Network: `file::list` | 11 | 11 |
| 10 | Network: `file::copy` | 8 | 8 |
| 11 | Network: method `.put()` | 8 | 8 |
| 12 | Network: method `.get()` | 7 | 7 |
| — | every other blocker | ≤ 4 each | |

By bar: Opaque 169, Write 105, Network 97, Live 69 (all with a write), Schema 40, Script 29,
Speculation 4, Model 2, Sleep 2.

### Seams the measured statements reach

| seam | statements |
|---|---:|
| Storage | 3229 |
| Permissions | 3034 |
| TreeCode | 1625 |
| Catalog | 1251 |
| Link | 957 |
| Params | 909 |
| Clock | 30 |

The kernel items reached most: `CallAllowed` 1542, `Scan` 1443, `Access` 865, `LoadParam` 663,
the program's dispatch 572, `Record` 440, `Lookup` 437, permission frames 367, `Values` 363,
`Index` 270, `MakeClosure` 227, `Block::Nested` 222, `MethodClosure` 207, `Subquery` 156,
`Recurse` 136.
