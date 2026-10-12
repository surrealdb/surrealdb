# RFC: Control flow and transaction boundaries in the logical plan

**Status:** Draft · **Scope:** `expr/src/expr/logical/` (ops, lowering, exit summaries),
`core/src/exec/logical/` (footprints, rules), `core/src/exec/vm/` (map, lower_block, speculate),
`core/src/dbs/executor.rs` (the request driver) · **Builds on:** `RFC_PRECOMPILED_QUERIES.md`
(catalog pinned per query, units, triage item T8), `VM_BLOCKS.md` · **Takes concepts from:** the
RFC "Surrealdb IR: Miro" (§10 states which, and why the rest is not adopted)

## 1. Problem

The logical plan describes what a statement computes. It does not describe how control leaves a
statement, or where a transaction starts and ends. Each later layer reconstructs that structure
for itself, at a different time and with different rules.

**Control flow is scalar.** `LogicalOp` (`expr/src/expr/logical/mod.rs:487-546`) has
`Sequence`, `Let`, `Return { what, fetch }` (`mod.rs:1318-1321`) and `Foreach`
(`mod.rs:1325-1329`) as operators. `IF`, `BREAK`, `CONTINUE` and `THROW` are not operators:
`lower` maps them to a scalar `LogicalExpr` wrapped in `LogicalOp::Eval`
(`lower.rs:250-255`, `lower_expr` at `lower.rs:306-309`). An `IF` whose branches hold statements
is therefore one scalar, `LogicalExpr::If` (`expr.rs:363`), whose branches are
`LogicalExpr::Subquery` of the branch plans (`lower_if`, `lower.rs:804-813`). No node records
whether it can return, throw, break or continue.

**Transactions are not in the plan at all.** `BEGIN`, `COMMIT` and `CANCEL` are
`sql::TopLevelExpr` variants that `TopLevelStatement::dispatch` routes to the driver
(`dbs/top_level.rs:49-66`). The executor discovers a block's extent at run time: on `BEGIN` it
calls `execute_begin_statement_inner` (`dbs/executor.rs:1817`), which pulls statements from the
request stream until it meets `COMMIT` or `CANCEL`. Misplaced control statements are run-time
errors raised by `run_surface_statement` (`executor.rs:1182-1205`). A `RETURN` inside a block
sets `skip_remaining` and the driver skips to the terminator (`executor.rs:2217-2219`); a
`BREAK` or `CONTINUE` there is `InvalidControlFlow` (`executor.rs:2221-2222`).

**Each top-level statement is planned when the driver reaches it.** `run_on_streaming_engine`
(`executor.rs:1028`) lowers and plans the statement in front of it, and `run_logical_plan`
(`executor.rs:1235`) hands a pre-lowered one to `plan_logical_inner` (`exec/planner.rs:3117`).
Only a block, a function body or a `FOR` loop is compiled whole (`vm/lower_block.rs:12-18`). The
test seal (`exec/compiles.rs`) counts compilations made while a program's stream is polled, so a
compile between two top-level statements does not trip it.

**The VM re-derives control flow while mapping.** The physical `Control` tree
(`vm/plan.rs:201`) already has statement-level `If`, `Throw`, `Break`, `Continue` and `Return`.
`Blocks::control` (`vm/map.rs:4458`) lifts them back out of the scalar: an `Eval` of
`LogicalExpr::If` becomes `Control::If` (`map.rs:4710-4717`, `if_else` at `map.rs:4745`), and
`if_else` decides branches whose condition the constants in scope settle. That is dead-branch
elimination, a logical rewrite, performed in the physical layer. `lower_block.rs` then turns the
tree into jumps and signals: `RETURN` emits `BlockInstr::Return` (`lower_block.rs:327`), which
raises `ControlFlow::Return` (`vm/block.rs:237`); the enclosing statement catches it according to
its `Collect` mode (`vm/program.rs:1043`, `block.rs:347-348`).

**Schema changes are modelled by running them.** A block's statements after a schema statement
are planned against an overlay written by a speculative run of that statement
(`vm/speculate.rs:1-39`). Where the change cannot be known ahead (under a data condition, in a
loop, through a body the block cannot see), the statements after it are planned late-bound
(`Planner::late_binding`, `exec/planner.rs:1746`).

**Footprints union every branch.** `footprint::of` (`exec/logical/footprint.rs:212`) walks every
plan nested in a statement, so an `IF` contributes the reads and writes of all its branches, and
the scalar `If` node itself adds nothing (`footprint.rs:396`). `Footprint::conflicts`
(`footprint.rs:192`) can only answer for the statement as a whole.

**Response safety is a driver property.** `rpc/src/stream.rs:7-16` defines the contract: `Rows`
and `Value` items are provisional until their statement's `Finished`, and a `Finished` carrying
an error retracts them. `finalise_results` (`executor.rs:382`) sends `Finished` once an outcome
can no longer change: after each auto-commit statement, and for a `BEGIN` block only once it
resolves. A request inside a session transaction that spans requests finalises per statement
(`executor.rs:2506-2508`). Consumers that forward early implement retraction
(`embedded/src/stream.rs:382-387`, `rpc/src/framing.rs:331`). The differential tests in
`core/src/dbs/stream_test.rs` (`streaming_matches_buffered`, `a_rolled_back_block_retracts_its_rows`,
`each_statement_is_terminated_exactly_once`, `dropping_the_consumer_stops_the_execution`,
`a_dropped_consumer_stops_later_statements`) pin it.

## 2. One program per request

The target is that **a request compiles to bytecode once, when it arrives, as one program**:

- The program's top level is a list of `Transaction` nodes (§3.2). Every statement of the request
  belongs to exactly one.
- The driver no longer walks `TopLevelExpr`. It runs the program, and at each `Transaction`
  boundary it opens the transaction, applies the node's commit or cancel, and applies the node's
  emission policy (§6). It never plans.
- Each `Transaction` node is compiled against a catalog known at arrival: the catalog pinned for
  the query (`RFC_PRECOMPILED_QUERIES.md` §4.2), plus the definitions written by the explicit
  transactions before it, plus its own schema statements, which later statements of the same
  node reach through unit calls rather than inlined code.
- Definitions that may differ when a node runs — changed by the request's own schema statements
  with run-time outcomes, or by another client between transactions — are reached through their
  version-keyed units rather than inlined (§4), so the language is unchanged.

The owner's layering rules hold throughout. The logical plan gains control flow and transaction
structure; it still knows nothing of indexes, storage or catalog contents. The physical plan owns
catalog reads and maps each `Transaction` node against its catalog. Bytecode translates the tree
mechanically and has no optimiser. Nothing compiles while the program runs, apart from the fault
path `exec/compiles.rs` already accounts for. A compiled `Transaction` node runs inside one
transaction and never outlives the snapshot it was planned against.

### 2.1 The shape of the program

The request's program is one `Program` value: the request's steps as data, one per statement
written (`BEGIN`, `COMMIT`, `CANCEL`, `OPTION`, `USE`, a top-level `LET`, a statement an engine
runs, and the surface statements the driver runs itself), and the program of every statement it
compiled. Every statement is lowered into one set of units — function bodies, closures, the call
units a statement links — which the request's program owns, and a statement's program indexes
them by id. The program encoding serialises the whole request, its steps included, and the read
frontier (§5.5) is extracted from it.

The driver executes the steps in order. Each step at the top level is a host action: opening,
committing or cancelling a transaction; recording a statement's result, its event and its slow-log
entry; flushing live-query notifications on commit; applying `USE`, `OPTION` or a top-level `LET`
to the session. None of them is something the VM evaluates. Making them interpreter steps would
only hand each one back to the host through a yield, and would turn the driver inside out around
the most intricate code it has (a block's failure, cancellation and timeout paths, retraction of
provisional rows). So the steps are data the driver walks, and a statement step runs that
statement's program by the same path a statement compiled on its own runs. Its rows stream to the
consumer and its value takes the shape it always has. A plain `EXPLAIN` keeps its own operator,
which renders the plan of the statement it explains.

## 3. Structured control flow in the logical plan

The plan stays a tree of structured nodes. A general control-flow graph is not proposed: every
SurrealQL construct is structured, a tree keeps the existing walkers (`visit_plans`, footprints,
the rule engine) working, and the VM already turns structured trees into jumps.

### 3.1 New and changed operators

```rust
pub enum LogicalOp {
    // … existing relational and write operators unchanged …
    Sequence(LogicalSequence),          // unchanged
    Let(LogicalLet),                    // unchanged
    Foreach(LogicalForeach),            // unchanged
    If(LogicalIfOp),                    // new: statement branches
    Return(LogicalReturn),              // now a terminator
    Throw(LogicalThrow),                // new terminator: { value: LogicalExpr }
    Break,                              // new terminator
    Continue,                           // new terminator
    Transaction(LogicalTransaction),    // new: only at the request's top level
}

pub struct LogicalIfOp {
    pub branches: Vec<(LogicalExpr, LogicalOp)>,  // condition, statement branch
    pub otherwise: Option<Box<LogicalOp>>,
}
```

- `If` is produced wherever an `IF` stands in statement position: a top-level statement, a block
  statement, a `FOR` body, a `LET` value, a branch of another `If`. An `IF` in value position
  inside an expression stays `LogicalExpr::If`. Its branches are scalars, and any branch holding a
  statement stays a `Subquery`, as today.
- `Throw`, `Break`, `Continue` and `Return` are terminators: control does not continue past them
  in their `Sequence`. Lowering drops any statement that follows a terminator in the same
  `Sequence`. It is unreachable, but it still parses and its errors are still reported.
- `LogicalEval` no longer carries `IF`, `THROW`, `BREAK` or `CONTINUE`. Its documentation
  (`mod.rs:1293-1294`) narrows to bare expressions.

### 3.2 `Transaction`

```rust
pub struct LogicalTransaction {
    pub mode: TxnMode,          // AutoCommit | Explicit { end: TxnEnd }
    pub body: Box<LogicalOp>,   // a Sequence for Explicit; one statement for AutoCommit
    pub emission: Emission,     // §6, decided at compile time
    pub first_index: usize,     // response index of its first statement
}
pub enum TxnEnd { Commit, Cancel, Unterminated }
```

- **AutoCommit** wraps one top-level statement. Its body is that statement's plan.
- **Explicit** wraps the statements between `BEGIN` and its terminator. `Unterminated` is a
  `BEGIN` with no `COMMIT` or `CANCEL` before the request ends. Today the driver cancels it at
  the end of the stream (`executor.rs:1833-1840`, falling through to the missing-terminator tail), and the node
  records that the same way.
- `BEGIN` inside `BEGIN`, and `COMMIT` or `CANCEL` outside a block, become lowering errors on
  the statement where they occur, with the messages `transaction_control_error` raises today
  (`executor.rs:1209-1213`).
- `Transaction` appears only as a direct child of the request root, which is a `Sequence` of
  them. A rule or walker that meets one elsewhere is a bug, and the plan validator rejects it.
- The response index of each statement is fixed by lowering (`first_index` plus position), so
  `BEGIN`, `COMMIT` and `CANCEL` keep their own result slots as today.

### 3.3 Exit summaries

Each node carries an `Exits` summary, computed bottom-up by lowering and kept beside footprints:

```rust
pub struct Exits {        // each flag means "may"
    pub falls_through: bool,
    pub returns: bool,
    pub breaks: bool,
    pub continues: bool,
    pub throws: bool,     // an explicit THROW only; evaluation errors are always possible
}
```

| Node | Rule |
| --- | --- |
| leaf statement | `falls_through` |
| `Return` / `Throw` / `Break` / `Continue` | that flag only |
| `Sequence` | walk in order; stop at the first child without `falls_through`; union the rest |
| `If` | union of branches; `falls_through` if there is no `otherwise` |
| `Foreach` | body's flags; `breaks` and `continues` absorbed, both becoming `falls_through` |
| `Let` value, `Return` value | `returns` absorbed (the value catches it, as `Collect::Let` / `Collect::Return` do) |
| `Transaction` | body's flags; `returns` absorbed (skips to the end of the block) |

The summary is what lets the plan state, at compile time, facts the driver and the VM now find
at run time: a `BREAK` that escapes every loop of a top-level statement is known when the request
compiles, and is still raised where `main` raises it, when the statement runs, after the
statements before it have run; and a `RETURN` in an explicit transaction ends the block's
statements there (the `skip_remaining`
behaviour, `executor.rs:2217`). It also lets a statement past a non-falling-through node be
dropped as dead code.

### 3.4 Dependence between statements

The statements of a `Transaction` node, and the nodes of a request, are ordered by the request
text. Most of that order is not observable. The plan records the order that is, as explicit
dependence edges between sibling statements, derived from footprints:

- Two statements depend on each other when `Footprint::conflicts` says so: one writes a resource
  the other reads or writes. A footprint's resources (a table, a record range, a catalog object,
  a session parameter) are the per-resource state splitting the Miro RFC names as a refinement of
  its single state edge; here they exist already.
- A statement that may fail, or whose result is non-deterministic (`rand::*`, `uuid`, the clock
  as ADR 0006 fixes it), keeps its place relative to every other such statement and to every
  write: the first error a request raises, and the values it draws, are the ones `main` produces.
- An `If` contributes the union of its arms' footprints to edges outside it, and each arm's own
  footprint to edges inside the arm (§5.1).
- A `Transaction` boundary orders everything: nothing moves across one.

With the edges explicit, reordering two statements, running independent statements in parallel,
and deciding whether a loop body may fan out are local checks on one edge or one node, not a
re-derivation from the whole statement list. The edges are a logical property: they are computed
from footprints and effects, with no catalog, and the physical and bytecode layers read them.

### 3.5 Expressions inside a statement

The statement tree stays a tree. The pure expressions inside one statement — a predicate, a
projection, a data clause, an `IF` condition, a `FOR` range — are held as a hash-consed DAG:

- **Global value numbering.** Two occurrences of the same pure expression in one statement are
  one node, evaluated once where its first occurrence dominates the rest (see *Errors keep their
  order*). Hash-consing gives this without a pass.
- **Dead-code elimination.** A node no root of the statement reaches is not lowered.
- **Local rewrite rules to a fixed point.** The expression rules of `exec/logical/rules` (constant
  folding, `IF` collapse, cast elision) apply node by node until a pass changes nothing, the same
  driver `optimise.rs` runs over operators.
- **Forward type inference.** Each node's kind is computed from its inputs'. The logical plan
  infers only proofs: kinds that follow from a literal, or from a value the query itself computes
  (a constructor, a cast, a comparison, a function whose return kind is fixed). A proof may fold a
  dynamic check (`$v.f` over a value known to be an object needs no array branch; a cast to the
  kind a value already has goes) and may let a rewrite move or drop an expression it shows
  cannot fail. The logical plan never reads a schema kind: kinds that come from the catalog or
  from storage are the physical plan's (§5.2). `LogicalExpr::kind` and `checked_kind` are the
  start of this; the DAG makes it one pass per statement rather than a query per call site.
- **Errors keep their order.** A logical rewrite may move or drop an expression that can fail
  only where a proof shows it cannot fail. Otherwise the expression stays where its evaluation
  order puts it, so the first error a statement raises is the one `main` raises. Merging two
  occurrences of one expression is not a move when the first dominates the second: the first is
  evaluated unconditionally, in the same evaluation region and against the same row, before every
  run of the second; no region between them catches or converts its error; and the expression is
  pure. Then any error the second would raise, the first has already raised at the point `main`
  raises it, and any value the second would compute, the first has computed. Such a merge needs no
  proof that the expression cannot fail, and a fallible pure expression (`a * 2`, `<int> v`) is
  shared on that ground alone.

The DAG lives inside a statement and never crosses a statement boundary, a `LET` the program
reads by name, or a call into a body the plan cannot see. Within a statement, a value is shared
only inside one evaluation region of the lowered program: one pipeline's row code, one data
clause, one `IF` condition chain, one `FOR` range.

**Unified primitives.** A `WHERE` clause, an array filter idiom `$arr[WHERE …]` and a call of
`array::filter` with a closure written in the call compute the same thing; they lower to one
`Filter` over their input. A projection and an array map (`$arr.map(…)`, `$arr.*.f`) lower to one
`Map`. A rule written for `Filter` or `Map` then covers every surface form, and the physical
layer's choices (an index for a `Filter` over a `Source`, a pipeline for a `Map` over rows) are
made once.

`FOR` stays a `Foreach` over a body region with no loop node of its own: the range is evaluated
once and the body runs per element, which is the shape a SurrealQL loop has.

### 3.6 Lowered shapes

**A schema change and a read.**

```surql
BEGIN; DEFINE FIELD n ON user TYPE int DEFAULT 0; UPDATE user SET n = 1; COMMIT;
SELECT n FROM user;
```

```text
Sequence
├─ Transaction { Explicit { end: Commit }, emission: Stream, first_index: 0 }
│  └─ Sequence
│     ├─ Ddl(DEFINE FIELD n ON user …)        exits {falls_through}
│     └─ Write(UPDATE user SET n = 1)         compiled against arrival + DEFINE FIELD
└─ Transaction { AutoCommit, emission: Stream, first_index: 4 }
   └─ Project[n](Source user)                 compiled against arrival + T0's definitions
```

**Statement branches inside a loop.**

```surql
FOR $o IN (SELECT * FROM order WHERE open) {
    IF $o.total > 100 { UPDATE $o.id SET tier = 'gold'; CONTINUE; };
    IF !$o.customer { THROW 'orphan' };
    UPDATE $o.id SET tier = 'std';
};
```

```text
Transaction { AutoCommit, emission: Stream }
└─ Foreach $o in Subquery(Filter[open](Source order))      exits {falls_through, throws}
   └─ Sequence                                             exits {falls_through, continues, throws}
      ├─ If [$o.total > 100]
      │  └─ Sequence [Write(UPDATE $o.id tier=gold), Continue]  exits {continues}; writes order
      ├─ If [!$o.customer]
      │  └─ Throw('orphan')                                     exits {throws}; writes nothing
      └─ Write(UPDATE $o.id tier=std)                           writes order
```

**A block ended early by `RETURN`.**

```surql
BEGIN;
LET $b = (SELECT VALUE bal FROM ONLY account:b);
IF $b < 10 { RETURN 'insufficient' };
UPDATE account:b SET bal -= 10;
COMMIT;
```

```text
Transaction { Explicit { end: Commit }, emission: Stream }   exits {falls_through}
└─ Sequence
   ├─ Let $b = Subquery(…)
   ├─ If [$b < 10] └─ Return('insufficient')   exits {falls_through, returns}
   └─ Write(UPDATE account:b …)                reached only when the If falls through
```

## 4. The catalog across transaction boundaries

The language does not change. Every request that runs on `main` runs the same way: an auto-commit
`DEFINE TABLE x; CREATE x SET a = 1;` works, a schema statement may appear anywhere a statement
may, and a later auto-commit statement sees definitions another client committed after the
earlier ones ran.

Each `Transaction` node is compiled against the catalog it can know at arrival, and reaches every
definition that could differ when it runs through that definition's unit instead of inlined code:

- **The request's own schema statements** are modelled at plan time. The physical planner builds
  each later node's catalog from the earlier nodes' schema plans (what `vm/speculate.rs` models
  within a block today, applied across nodes). Where a schema statement's outcome is decided at
  run time — inside an `If`, `IF NOT EXISTS` or `OVERWRITE` against an existing definition, a
  statement that may fail — the later statements that read the definitions it touches call their
  units, so they read whichever definition the run produced.
- **Other clients' schema statements** between two auto-commit nodes are visible to the later
  node, as on `main`. Only the first node runs at the snapshot its catalog was read from, so only
  the first node inlines catalog definitions; later nodes call the units of the definitions they
  use. Units are served from the compile cache by catalog object and version (`RFC_PRECOMPILED_QUERIES.md` §3), so a later node meets
  a changed definition as a cache lookup at its transaction boundary, never as a compile inside a
  running program.
- **Explicit transactions** behave the same: a node after a `BEGIN … CANCEL`, or after a `COMMIT`
  that fails, calls units for the definitions the rolled-back node touched.

What this keeps: one compile of the request at arrival; nothing compiles while a program runs;
every node runs within the snapshot of its own transaction. What it costs: statements in later
nodes call units where the first node would inline, and a definition another client changed
between nodes is compiled into the cache the first time it is served. Both are measured in the
migration (§8).

A schema statement reached only through a body the plan cannot see (a stored function, an event,
a `surrealdb.query()` call from JavaScript) widens the footprint to `Unknown`; the statements after
such a call in the same node call units for every definition they use.

### 4.1 The current mechanism: statement units

Which access path a statement takes — a KNN, full-text or B-tree index, a union of index scans, a
count index — and what follows from it above the source (a residual filter stripped, a sort the
index order covers, a limit pushed to the scan, the distances `vector::distance::knn()` reads, the
scope `search::score` reads) is decided by the physical planner from the catalog. A statement
planned before a schema change it then meets cannot take that path from units alone. The
mechanism that exists today is a **statement unit**:

- The statements of the first transaction before any statement that may change the catalog — a
  schema statement, a write, a `USE`, a `LIVE` or `ACCESS`, anything whose effects are not known —
  are compiled into the request's program when it arrives and inline the catalog.
- Every other statement, at the top level or as a late-bound statement of a block
  (`BlockInstr::RunServed`), is planned when it is reached for the catalog its transaction reads
  then, and served from the compile cache keyed by its text, the names bound around it and the
  principal's frame class. It is checked against the catalog entries its compilation read and the
  context values it folded; the session's own values (`$auth`, `$session`, `$token`, `$access`)
  are read when it runs, never folded. The program holds no logical plan: the operator that runs
  it keeps the statements as written beside it.
- A statement whose plan must not outlive its run is planned when it is reached, for that run
  alone: an `EXPLAIN`, whose operator renders what that run measured; a statement reading the
  session's own values, which `main` folds as the statement's own; and one holding a schema
  statement, which planning models with a speculative run in the transaction it runs in.

So a compile happens only at a statement, after a real catalog change or on the cache's first
sight of a statement, never inside a pipeline; a repeated request is served the plans kept for it.
The compile counters keep cache fills (`exec::compiles::faults`) apart from planning at a
statement (`exec::compiles::after_arrival`). This matches `main` by construction, since each
served statement is `main`'s planning at the catalog the statement runs against.

Selecting the access path at source open, from the table's descriptor, so that later statements
call version-keyed units and nothing is planned after arrival, remains the later option. It needs
the choice and everything that depends on it above the source to be made at run time.

## 5. What each layer gains

### 5.1 Logical rewrites

- **Per-branch footprints.** `footprint::of` gives each `If` branch its own footprint, and the
  `If` reports the union for callers that need the whole. A rewrite that moves a statement across
  an `If` still uses the union. A rewrite inside a branch (hoisting an invariant out of a loop
  body, reordering two branch statements, eliminating a redundant read) consults only that
  branch. `conflicts()` keeps its signature; it is called with smaller footprints.
- **Dead code.** Branch folding moves out of `map.rs::if_else` into a logical rule: a condition
  that folds to a constant selects its branch, and a statement after a node with
  `!falls_through` is removed. The physical layer stops deciding branches.
- **Which nodes may inline catalog definitions** (§4) is decided from `Transaction` nodes and
  footprints: the first node, and definitions no earlier node or other client can change before a
  node runs. It needs no catalog.

### 5.2 Physical planning

- **Kinds from the catalog and from storage.** The physical plan owns schema- and layout-derived
  kinds, read from the catalog it planned against, under the snapshot the program runs in. A
  declared kind is a guarantee only where that catalog guarantees every row it applies to: a
  table whose rows are all in one packed layout generation, with no older generation left, as the
  table's floor and conformance record say; or a packed row read through its generation's
  descriptor, which makes the guarantee when the row is read. Where the guarantee
  holds, the physical plan may drop checks, reorder or merge fallible expressions, and bind
  monomorphic fast paths without a per-row guard. Where it does not — `SCHEMALESS` tables,
  `FLEXIBLE` fields, `any` and `option<…>`, revision-encoded rows or rows of an older generation,
  a field type defined or changed after rows existed, which does not revalidate them — a declared
  kind is a hint: it may select a fast path guarded by a check, with the general path as the
  fallback (the descriptor fast/slow branch the VM already uses), and nothing else.
- **Cost-based choices later.** Index selection, join order and top-k placement are choices with
  a cost, not strict improvements, so they are not logical rules. Once statistics exist
  (`RFC_STATISTICS_AND_COST.md`), the physical layer may hold the alternatives for one statement
  as an e-graph and extract the cheapest; until then it chooses as it does today.

- `Blocks::control` stops lifting `Control::If` and `Control::Throw` out of `LogicalExpr`
  (`map.rs:4710-4723`) and the `Eval` special cases for `Break` and `Continue` in
  `Blocks::statement` go too. It maps operators to `Control` one for one.
- **Morsel fan-out per branch.** Fan-out eligibility is decided from footprints and effects. With
  the union, one branch that writes outside its row disqualifies a loop body whose other branches
  are row-local. Per-branch footprints let the physical plan fan out the eligible branch and run
  the rest in place, or at least tell the two apart in `EXPLAIN`.

### 5.3 One compile per request, and what `speculate.rs` reduces to

Each `Transaction` node is mapped against its catalog: arrival, plus earlier explicit
transactions' definitions, plus its own. Inside an explicit node, a schema statement is followed
by statements planned the way the late-bound path plans them today: objects the schema statement
touches are reached through units by name and version (`RFC_PRECOMPILED_QUERIES.md` §4.1), and
everything else is inlined from the pinned catalog.

- Across nodes, `speculate.rs` is no longer needed. A later node's catalog is the earlier
  explicit nodes' definitions applied to the pinned catalog, which the physical planner builds as
  an overlay from the DDL plans without running them. Where an earlier explicit node rolls back,
  the facts compiled into the later node are stale. They are hints loaded through descriptors and
  units keyed by version (`RFC_PRECOMPILED_QUERIES.md` §5–6), so the code stays correct and only
  loses its specialisation.
- Within a node, speculation remains for one purpose: drawing catalog ids (`HeldIds`,
  `speculate.rs:21-28`) that later statements in the same node are compiled to use. If unit calls
  cover every statement after a schema statement, the overlay disappears and only the id
  reservation is left. Whether to go that far is §9.
- `Control::Statement::{planned_over, modelled, late_bound}` (`plan.rs:252-279`) become
  properties of the node's catalog rather than per-statement discoveries.

### 5.4 Bytecode

`lower_block.rs` becomes a mechanical translation of a tree into jumps. Its inputs are already
structured; what it loses is the inference. `If` lowers to `JumpIf` chains. `Break` and `Continue`
jump to the enclosing loop's labels, or are rejected by lowering when no loop encloses them, so
the "signal leaves the program" case (`lower_block.rs:427`) is limited to function bodies.
`Return` jumps to the catch target that the exit summary names (the enclosing `Let`, `Return` or
`Transaction` end) instead of raising `ControlFlow::Return` and relying on `Collect`.
`Transaction` lowers to `TxnBegin` / `TxnEnd` instructions that yield to the driver.

### 5.5 Prefetch and shipping bytecode

Both need the whole request as one program; a program compiled one statement at a time
supports neither.

- **Prefetch.** A compiled request is linear code with known key ranges: each `Source` of each
  pipeline, in every `Transaction` node, names the ranges it will read, with parameters folded
  where the plan folds them. Lowering derives the request's *read frontier* — the ranges every
  later pipeline reads — and emits it with the program. When a transaction opens, the host hands
  the frontier for that node to the KV layer as a prefetch hint, ahead of the first row any
  pipeline reads. A hint sits below the transaction: reads still go through the transaction and
  see its own writes, so a hint changes latency, never a result. Ranges that depend on a value
  computed at run time are hinted when the instruction that computes them runs. The KV layer
  needs a hint operation (`EDGE_PLAN.md`, rule 10); without one the frontier is unused. On object
  storage, where each read is a round trip whatever its size, issuing the frontier's reads
  together is the main win.
- **Shipping bytecode to fans.** A fan runs a block (`EDGE_PLAN.md`, W6): an encoded program for
  one pipeline, with no logical IR, keyed by build and carrying a capability manifest. With one
  program per request, every block of the request exists when the request arrives. The root can
  therefore encode and address each block once, ship blocks to fans ahead of the morsels that use
  them, and place work for the whole request rather than statement by statement. A fan never
  compiles; a fan that cannot serve a block's manifest hands the morsel back to the root.
- **What the program must carry for both:** a versioned, build-keyed encoding of the program and
  its blocks (instructions, constants, descriptor and unit references by catalog object and
  version, the `Transaction` structure, and each block's manifest), and the read frontier per
  `Transaction` node. The program's in-memory form is plain data today but has no stable encoding;
  adding one is part of this work.

## 6. Response emission

The driver runs the program and acts at `Transaction` boundaries. The provisional-row protocol of
`rpc/src/stream.rs` is unchanged and remains the contract. Every statement ends with exactly one
`Finished`; rows before it are provisional; an errored `Finished` retracts them.

Each `Transaction` node carries an **emission policy**, decided at compile time:

- **`Stream`** (current behaviour): rows go out as produced. `Finished` goes out per statement
  for `AutoCommit`, and for every statement of an `Explicit` node when it resolves.
- **`Hold`**: rows are buffered and sent, each statement followed by its `Finished`, after
  `COMMIT` succeeds. Nothing is ever retracted.

Defaults and constraints:

- Every node defaults to `Stream`, which produces the same response stream items as `main`, item
  for item.
- `AutoCommit` is always `Stream`. Nothing after the statement can rewrite it.
- `Hold` is chosen only when the request or session asks for it. For `Explicit { end: Cancel }`
  and `Unterminated` it sends only the errors, never rows that will be retracted; that changes the
  item sequence a consumer sees (the final results are the same), so it is not a default.
- Session transactions that span requests keep finalising per statement (`executor.rs:2506`),
  since the owner commits in a later request. They are modelled as a third mode, `Session`, whose
  emission is always `Stream`.

**Trade-off.** `Stream` gives the first row sooner and bounded server memory, because the
consumer's read rate limits the producer (`QUERY_STREAM_BUFFER`, `stream.rs:32`). The cost is
that every consumer forwarding rows before `Finished` must implement retraction: the embedded
SDK and the WebSocket framing do, and third-party clients and proxies must. `Hold` removes
retraction for the node and delays the first row until `COMMIT`. Buffered, it costs memory
proportional to its rows, so a large node needs the spill path or a limit that turns it into an
error. §6.1 bounds it for the statements whose output can be re-read after the commit.

The differential tests become the acceptance test for the driver change:

- `streaming_matches_buffered` and `each_statement_is_terminated_exactly_once` run under both
  policies.
- `a_rolled_back_block_retracts_its_rows` keeps asserting retraction under `Stream`, and gains a
  twin asserting that `Hold` emits no rows.
- The dropped-consumer tests keep their guarantee: an abandoned explicit node rolls back
  (`executor.rs:1834-1840`).

### 6.1 Bounded `Hold`: deferred yield

A buffered `Hold` node holds every row it produces until `COMMIT`, so its memory grows with its
result. A statement can instead be split into two pipelines:

- an **effect** pipeline, run in place inside the transaction, which performs the writes and
  records the keys the statement touched, but emits no rows;
- a **yield** pipeline, run after `COMMIT` succeeds, which re-derives the statement's rows by
  reading those keys at the commit's version and streams them under the normal backpressure.

Nothing is held but the touched keys, and nothing is ever retracted. The yield reads state again
in place of buffering rows, so it is correct only where re-reading reproduces what the statement
would have returned at its own point in the transaction. A statement defers its yield only when
all of these hold:

- **No later write in its footprint.** No statement after it in the same `Transaction` node
  writes a resource its output reads (§3.4). A KV transaction keeps no snapshot partway through,
  so the commit version shows the final state, not the state at this statement.
- **Output determined by its keys and that state.** The projection is deterministic (no
  `rand::*`, `uuid`, clock reads or effectful calls), and the output is the state after the
  statement (the `RETURN AFTER` default, or a `SELECT`). `RETURN BEFORE` and `RETURN DIFF` need
  the before-image, which the write has replaced.
- **A store that supports versioned reads.** Reading at the commit version keeps another
  client's later write out of the yield. The KV trait takes `version: Option<u64>`, but the
  memory and IndexedDB backends reject versioned reads, so on those stores every `Hold` statement
  buffers.

A statement that fails any condition buffers its rows, spilling to the temp store past the
memory limit. The touched-key record is far smaller than the rows, and it grows with the
statement's write count, so it spills under the same limit. Eligibility is a logical property
read from dependence edges and effect summaries. Version support is a physical one, read from the
datastore when the node is compiled.

## 7. Relation to existing work

Late-bound planning already exists: the statements after a schema statement inside a block are
planned at arrival against the table they meet, and their writes run through stage bundles, call
units and parameter units, with no deferred compile at run time. That work stops at the block
boundary: it makes one compile per *block*. This RFC moves the boundary to the request. It reuses
late-bound planning for the code after a schema statement inside an explicit node (§5.3), and
replaces the per-statement planning in `run_on_streaming_engine` / `run_logical_plan` with one
compile of the `Transaction` sequence.

## 8. Migration plan

Each step is one PR, behaviour-preserving unless stated, and green on the language tests and the
stream differential tests. No persisted format changes: plans, units and the compile cache are
keyed by build, so no upgrade test is required. If a step makes a unit's ABI depend on the new
nodes, the compile-cache key already isolates it.

1. **`If`, `Throw`, `Break`, `Continue` operators.** Lowering emits them in statement position;
   `map.rs` maps them one for one; `LogicalEval` narrows. Tests: lowering unit tests for every
   position (top level, block, `FOR`, `LET`, nested `If`); VM goldens unchanged; the full
   language suite.
2. **Exit summaries.** Computed in lowering. An escaping `BREAK` / `CONTINUE` lowers to the same
   run-time error `main` raises, at the same point, with the same message; nothing is rejected
   before the request runs. Tests: a summary table test per
   node kind; language tests for each misplaced construct, asserting the message.
3. **Per-branch footprints and dead-code rule.** Branch folding moves from `map.rs::if_else` into
   a logical rule. Tests: footprint unit tests for branch-local writes; rule tests; `EXPLAIN`
   goldens showing folded branches.
4. **`Transaction` nodes.** Lowering of the whole request produces them. The driver still walks
   statements but reads boundaries and response indices from the plan, and misplaced
   `BEGIN`/`COMMIT`/`CANCEL` are reported by lowering. Tests: the stream differential tests
   unchanged; language tests for nested `BEGIN`, stray `COMMIT`, unterminated blocks, and
   `RETURN` inside a block.
5. **Emission policy.** `Stream` everywhere by default; `Hold` when the request asks for it. Tests:
   the default stream differential tests unchanged; the `Hold` twins of the differential tests;
   framing tests in `rpc` and `embedded` showing a held node emits no retraction. This step
   buffers held rows; deferred yield (§6.1) needs the dependence edges of step 10 and follows it.
6. **The catalog across transaction boundaries** (§4), with no change to the language. Tests:
   language tests for auto-commit schema statements followed by use in one request (table, field,
   index, function, param, `ALTER`/`REMOVE`, `IF NOT EXISTS`/`OVERWRITE` outcomes, a failing
   `DEFINE`), and a Rust test with two clients in which client B's schema statement lands between
   client A's auto-commit statements and A's later statement sees it, as on `main`.
7. **One program per request.** The driver compiles the `Transaction` sequence at arrival and runs
   it; per-statement planning in `run_on_streaming_engine` goes. `speculate.rs` loses its
   cross-node role. Tests: a compile-count test over a multi-statement request asserting one
   compile (extending `exec/compiles.rs` with a per-request count), plus the full language,
   stream and SDK suites.
8. **`lower_block.rs` simplification.** `Return` lowers to jumps; `Collect`-based catching
   remains only for function bodies. Tests: VM goldens; `tests_ctl.rs`.
9. **Program encoding and read frontier** (§5.5). A versioned, build-keyed encoding of the request
   program and its blocks, and the read frontier per `Transaction` node. Tests: round-trip of every
   VM golden through the encoding with identical results; a decoder rejecting another build's
   blocks; a frontier test asserting the ranges a multi-statement request reads are all in its
   frontier. Fans and the KV hint operation consume these in `EDGE_PLAN.md` W6. This step is the
   leaf-readiness work's, alongside the block run-time frame and host seams.
10. **Dependence edges** (§3.4). Computed from footprints and effects; the existing reorderings and
    fan-out eligibility read them. Tests: edge unit tests per resource kind; a test that a failing
    or non-deterministic statement keeps its place; stream differential tests unchanged.
11. **Expression DAG and type inference** (§3.5, §5.2). Hash-consed per statement; the expression
    rules run over it; logical proofs fold dynamic checks; the physical plan uses declared kinds
    as guarantees only where its catalog guarantees them, and as guarded hints elsewhere. Tests:
    GVN and DCE unit tests; a test per fallible expression kind that a merge or move happens only
    when a proof or guarantee shows it cannot fail; a table whose field type was defined after
    rows of another kind were written, read through a literal-proven path, a guarded hint and a
    strict-layout descriptor, with `main`'s results and errors; VM goldens updated only where an
    evaluation is shared.
12. **Unified `Filter` and `Map`** (§3.5). Tests: one rule test per surface form reaching the same
    node; the language suite.

## 9. Open questions

0. **Hold by default for rollbacks.** Sending only errors for a `CANCEL` block removes retraction
   for it, but changes the item sequence a streaming consumer sees. Is that a change worth a
   release note, or does `Hold` stay opt-in?

1. **Unit-call cost in later nodes.** How much slower is a later node that calls units where the
   first node inlines, on the bench shapes? If it matters, which definitions can be proven
   unchanged at a boundary without a compile (a version check per definition at transaction open
   selecting between two compiled forms)?
2. **Unseen bodies.** Can a stored function's effect summary narrow `Unknown` to the definitions it
   may write, so statements after its call keep inlining the rest?
3. **Rolled-back predecessors.** Is calling units for every definition a rolled-back node touched
   the simplest correct form, or is a per-node second compiled form (assuming rollback) cheaper?
4. **Streamed requests.** `execute_stream` takes statements as a stream (`executor.rs:2516`), and
   `surreal import` relies on it. One compile per request needs the whole request. An import is
   either compiled in chunks at explicit-transaction boundaries or kept on a per-statement path.
5. **`Hold` selection.** Per request, per session, or inferred? What memory bound turns a held
   node into an error, and is that error a statement's or the `End` frame's?
6. **Session transactions.** A request inside a client-held transaction may run DDL whose
   `COMMIT` arrives in a later request. The next request's catalog is read at its arrival, inside
   the same transaction; confirm the unit-call rule of §4 covers it.
7. **Id reservation without overlays.** If unit calls cover every statement after a schema
   statement inside a node, can `speculate.rs` shrink to id reservation alone?
8. **`LET` across nodes.** Top-level `LET` binds a session-visible parameter between auto-commit
   nodes. Its value flows from one transaction to the next. It needs to be a node output the
   program carries, not a driver side-effect.
9. **Deferred yield through the fans.** Can a fan serve a read at a given commit version, so a
   deferred yield (§6.1) reads through the fanned serve rather than the coordinator's store? If
   not, the yield reads the store directly.
10. **The touched-key record.** A statement that writes a contiguous key range could record the
    range instead of each key. Is that worth a second record shape, or is a spillable key list
    enough?

## 10. Relation to "Surrealdb IR: Miro"

The Miro RFC proposes representing a whole request as one dependence graph in the style of RVSDG:
a `lambda` over a threaded state value, `switch` regions for `IF`, `ForEachArray` over a `lambda`
for `FOR`, `RETURN`, `BREAK` and `CONTINUE` as predicates the parent scope tests, statements
decomposed into primitives (`Scan`, `Filter`, `Map`, `MakeObject`, `ObjectGet`, `ConcatArray`,
`Set`), hash-consing for value numbering and dead code, forward type inference, local rewrite rules
to a fixed point, and e-graphs later.

**Adopted.**

1. The whole request as one unit (§2).
2. Explicit dependence between statements in place of a total order, with per-resource splitting
   (§3.4). Footprints already give the resources Miro leaves as a refinement.
3. Inside a statement, pure expressions as a hash-consed DAG with value numbering, dead-code
   elimination, local rewrite rules to a fixed point and forward type inference; schema and
   layout kinds are the physical plan's, guarantees only where its catalog guarantees them (§3.5,
   §5.2).
4. Unified primitives: one `Filter` and one `Map` for every surface form that computes them
   (§3.5).
5. `FOR` as an iteration over a body region rather than a loop node (§3.5).
6. E-graph extraction with costs, as a later option of the physical layer (§5.2).

**Not adopted, and why.** The statement level stays the structured tree of §3:

- **Errors are not modelled.** In SurrealQL an auto-commit statement's error does not stop the
  request; an error inside `BEGIN … COMMIT` cancels the transaction and turns the results it
  produced into errors; and which error a statement raises first must match `main`. A dependence
  graph without error edges cannot express any of this, and with them, in a dynamically typed
  language where nearly every operation can fail, almost every node is pinned in order and the
  freedom the graph exists for is gone. §3.5 keeps that freedom where it is real: pure
  expressions whose types prove they cannot fail.
- **Transactions need more than a state token.** Each auto-commit transaction opens a fresh
  snapshot that may include other clients' commits (§4), and a commit or cancel outcome changes
  what earlier statements' results are. `Transaction` nodes with exit summaries state both.
- **`Scan` as an array value fights streaming.** The executor streams rows under bounded memory,
  and the response protocol streams them to the client (§6). Value semantics for a table would
  have to be undone by every lowering.
- **Response order would need its own effect.** Results are emitted per statement, in order, with
  provisional rows and retraction. In a graph that is another threaded effect; in the tree it is
  the statement order.
- **`RETURN` as a returned predicate is unnecessary.** Structured exits (§3.3) give the same facts
  at compile time, and lowering turns them into jumps directly (§5.4).
- **Unproven, and its closest production relative chose a hybrid.** No database runs an RVSDG IR.
  Cranelift kept side-effecting operations in a control-flow graph and uses a dependence graph only
  for pure values. This RFC makes the same split: a structured tree for statements, effects,
  transactions and errors; a DAG for the pure values inside each statement.

