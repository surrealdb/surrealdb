# exec/ — Streaming Query Execution Engine

This module implements SurrealDB's streaming query execution engine. It replaces the legacy recursive `compute()` path with a push-based, batched operator pipeline.

## Architecture

```
SurrealQL (Expr) ──► Planner ──► ExecOperator tree ──► ValueBatchStream ──► Results
```

1. **Planner** converts parsed LogicalPlan (`expr::Expr`) into a tree of `Arc<dyn ExecOperator>`
2. **Executor** (`dbs::Executor::execute_operator_plan()`) builds an `ExecutionContext` and calls `plan.execute()`
3. Each operator returns a `ValueBatchStream` — an async stream of `ValueBatch` (Vec\<Value\>)
4. If the planner returns `PlannerUnsupported`/`PlannerUnimplemented`, execution falls back to `Expr::compute()` via `plan_or_compute`.

## Core Traits

| Trait | Purpose |
|-------|---------|
| `ExecOperator` | Physical operator that produces a `ValueBatchStream`. Declares context requirements, access mode, cardinality, ordering. |
| `PhysicalExpr` | Expression evaluation in streaming context. Per-row evaluation via `evaluate()` against an `EvalContext`. |
| `ScalarFunction` | Stateless function (pure, context-aware, or async) producing a single value per invocation. |
| `AggregateFunction` | Stateful function with an `Accumulator` for GROUP BY aggregation (count, sum, mean, etc.). |
| `ProjectionFunction` | Function that produces field bindings for output objects. |
| `IndexFunction` | Index-backed functions: MATCHES (full-text) and KNN (vector search). |

## Key Types

- **`ValueBatch`** — Batch of values (`Vec<Value>`). Designed for future columnar execution.
- **`ValueBatchStream`** — `Pin<Box<dyn Stream<Item = FlowResult<ValueBatch>> + Send>>` (non-Send on WASM)
- **`ExecutionContext`** — Hierarchical: `Root` | `Namespace(ns)` | `Database(ns, db)`. Holds transaction, params, auth, capabilities.
- **`EvalContext`** — Borrowed view for per-row expression evaluation. Carries current value, params, session info, recursion context.
- **`AccessMode`** — `ReadOnly` | `ReadWrite`. Determines transaction mode and dependency ordering barriers.
- **`CardinalityHint`** — `AtMostOne` | `Bounded(n)` | `Unbounded`. Drives buffering strategy selection.
- **`OutputOrdering`** — `Unordered` | `Sorted(...)`. Enables sort elimination when input is already ordered.
- **`FlowResult<T>`** — `Result<T, ControlFlow>` supporting return/break/continue/error signals through the operator tree.

## Module Layout

### Root files

| File | Purpose |
|------|---------|
| `mod.rs` | Core traits (`ExecOperator`, `ValueBatch`, `ValueBatchStream`), WASM-compat helpers |
| `planner.rs` | Entry point: `Planner::plan()` converts `Expr` → `Arc<dyn ExecOperator>` |
| `context.rs` | `ExecutionContext` hierarchy, `SessionInfo`, `ContextLevel`, `Parameters` |
| `physical_expr.rs` | `PhysicalExpr` trait, `EvalContext` |
| `access_mode.rs` | `AccessMode` enum and combine logic |
| `cardinality.rs` | `CardinalityHint` for buffering decisions |
| `ordering.rs` | `OutputOrdering` for sort elimination |
| `buffer.rs` | Stream buffering strategies based on cardinality and access mode |
| `metrics.rs` | `OperatorMetrics` and `monitor_stream` for EXPLAIN ANALYZE |
| `expression_registry.rs` | Expression deduplication for SELECT pipelines |
| `field_path.rs` | Pure field paths (no execution, used for sort/extraction metadata) |
| `index.rs` | Index analysis and access path selection |
| `permission.rs` | Permission resolution for tables/fields |
| `plan_or_compute.rs` | Fallback bridge: try streaming planner, fall back to legacy `compute()` |
| `parts.rs` | Physical part expressions for idiom evaluation |

### `planner/` — Query Planning

Converts Logical Plan into operator trees. Key submodules:

- `select.rs` — SELECT pipeline planning (FROM → WHERE → SPLIT → GROUP → ORDER → LIMIT → FETCH → PROJECT → TIMEOUT)
- `aggregate.rs` — GROUP BY and aggregate function extraction
- `source.rs` — Source planning (table lookup, index selection, record ID scan)
- `idiom.rs` — Idiom → physical part conversion
- `util.rs` — Literal conversion, condition resolution

### `operators/` — Physical Operators

Each file implements `ExecOperator`:

- **Scan**: `scan/table.rs`, `scan/index.rs`, `scan/record_id.rs`, `scan/fulltext.rs`, `scan/knn.rs`, `scan/graph.rs`, `scan/reference.rs`, `scan/dynamic.rs`, `scan/count.rs`, `scan/index_count.rs`, `scan/union_index.rs`, `scan/resolved.rs`
- **Pipeline**: `filter.rs`, `project.rs`, `project_value.rs`, `limit.rs`, `sort.rs`, `aggregate.rs`, `split.rs`, `fetch.rs`, `timeout.rs`, `union.rs`, `knn_topk.rs`
- **Control flow**: `ifelse.rs`, `foreach.rs`, `sequence.rs`, `return.rs`, `let_plan.rs`
- **Info**: `info/` — ROOT/NS/DB/TABLE/INDEX/USER info statements
- **Recursion**: `recursion.rs` — graph traversal with BFS discovery + bottom-up assembly
- **Explain**: `explain.rs` — EXPLAIN / EXPLAIN ANALYZE output
- **Bridge**: `expr.rs`, `compute.rs`, `source_expr.rs` — legacy fallback wrappers

Scan operators share a common record-processing pipeline in `scan/pipeline.rs` and common iterator logic in `scan/common.rs`.

### `physical_expr/` — Expression Evaluation

Implements `PhysicalExpr` for all expression types:

- `literal.rs` — Constants, parameters, mock expressions
- `idiom.rs` — Field access / path traversal
- `record_id.rs` — Record ID construction
- `ops.rs` — Binary, unary, postfix operators
- `conditional.rs` — IF/ELSE expressions
- `control_flow.rs` — RETURN, BREAK, CONTINUE, THROW
- `block.rs` — Block expressions
- `collections.rs` — Array, object, set literals
- `matches.rs` — MATCHES operator (full-text search)
- `subquery.rs` — Scalar subqueries
- `function/` — All function call types (builtin, closure, JS, ML model, user-defined, index, projection, Surrealism modules)

### `function/` — Function System

- `registry.rs` — `FunctionRegistry`: lookup by name, supports scalar + aggregate + projection + index functions
- `builtin/` — ~30 modules implementing scalar functions (math, string, array, object, record, type, geo, vector, search, crypto, http, time, duration, etc.)
- `builtin/aggregates.rs` — Aggregate functions (count, math::sum, math::mean, array::group, etc.)
- `aggregate.rs` — `AggregateFunction` trait and `Accumulator`
- `projection.rs` — `ProjectionFunction` trait
- `index.rs` — `IndexFunction` trait, `MatchesContext`, `KnnContext`
- `signature.rs` — Function signatures with argument types
- `method.rs` — Method-style call descriptors

### `parts/` — Physical Part Expressions

Evaluate idiom parts (field access, array indexing, etc.):

- `field.rs`, `index.rs`, `lookup.rs`, `filter.rs`, `array_ops.rs`, `destructure.rs`, `optional.rs`, `recurse.rs`, `method.rs`

### `index/` — Index Support

- `access_path.rs` — `AccessPath`, `BTreeAccess`, index selection logic
- `analysis.rs` — `IndexAnalyzer`, `IndexCandidate` for query optimization
- `iterator/` — Index iterator implementations

## Design Rules

1. **No `compute()` calls** — All evaluation goes through `PhysicalExpr` and `ExecOperator`. The only bridge to legacy `compute()` is in `plan_or_compute.rs` for not-yet-planned statement types.
2. **WASM compatibility** — Use `SendSyncRequirement` trait, `BoxFut<'a, T>`, and `#[cfg_attr(target_family = "wasm", async_trait(?Send))]` patterns.
3. **Access mode propagation** — Every operator and expression must correctly report its `AccessMode`. A SELECT with a mutating subquery must return `ReadWrite`.
4. **Context level validation** — Operators declare minimum `ContextLevel` (Root/Namespace/Database). The executor validates before execution.
5. **Batched processing** — Operators work with `ValueBatch`, not individual values. This amortizes per-record overhead and enables future columnar execution.

## SELECT Pipeline

The standard SELECT pipeline is:

```
Scan/Union (FROM)
    → Filter (WHERE)
    → Split (SPLIT BY)
    → Aggregate (GROUP BY)
    → Sort (ORDER BY)
    → Limit (LIMIT/START)
    → Fetch (FETCH)
    → Project (SELECT fields)
    → Timeout (TIMEOUT)
```

Sort elimination: if the scan already produces ordered output (e.g., index scan), the planner checks `OutputOrdering` and skips the Sort operator.

## Entry Points

- **`Planner::plan(expr)`** / **`try_plan_expr!(expr, ctx, txn)`** — Plan an expression into an operator tree
- **`Planner::with_txn(ctx, txn, ns, db)`** — Planner with transaction for index resolution at plan time
- **`plan.execute(&exec_ctx)`** — Execute operator tree, returns `ValueBatchStream`
- **`Executor::execute_operator_plan(plan, txn)`** — Full execution: build context, run plan, collect results
- **`expr_to_physical_expr(expr, ctx)`** — Convert an expression Logical Plan node to a `PhysicalExpr`
