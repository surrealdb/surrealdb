# ADR 0007: One engine, the bytecode VM

**Status:** Accepted · **Scope:** `surrealdb/core/src/exec/`, `surrealdb/core/src/dbs/executor.rs`,
the engine and strategy switches of the server and the language-test runner

**Supersedes:** ADR 0005 (the per-record engine stays), and ADR 0002's position that there is no
plan-level VM.

## Context

Three executors answer SurrealQL.

- **The per-record engine** (`legacy/`, the document pipeline in `doc/`, and `dbs::Iterator`),
  which evaluates an `expr::Expr` one record at a time.
- **The operator tree** (`exec/operators/`), a push-based tree of `ExecOperator`s exchanging
  `ValueBatch`es, compiled from a bound `LogicalOp`.
- **The bytecode VM** (`exec/vm/`), which maps the bound plan to an inert `PhysicalPlan`, lowers it
  to a program and runs it on a synchronous interpreter under an async host (ADR 0004).

Two switches chose between them. `SURREAL_PLANNER_STRATEGY` (`compute-only`, `best-effort`,
`all-read-only`) decided which statements the streaming path was offered, and `SURREAL_EXEC_VM`
decided whether the VM was offered each streaming plan before the operator tree was compiled. A
statement the VM could not lower fell to the operator tree; one the streaming path could not plan
fell to the per-record engine. `SURREAL_PLANNER_STRICT` and `SURREAL_VM_STRICT` measured how often
either happened, and the language suite ran every test under each strategy and failed on any
divergence between them.

ADR 0002 recorded an operator-tree-only direction in another branch: the plan layer stays a tree of
operators, with no plan-level bytecode VM. ADR 0004 diverged from it. ADR 0005 then kept the
per-record engine as permanent, so all three executors stayed, each with its own copy of every
semantic the language has.

The cost of that arrangement is the problem this ADR removes. Every semantic rule lives in up to
three places that must agree, a change to one is checked against the others by a differential suite
that is also the largest part of the suite's run time, and a decline invisibly routes a statement
to an executor nobody chose. The VM covers the language: the statements that still reach another
executor are a short list of gaps (nested schema statements, `USE` arguments, live queries, the
stored-API path, closures and the host entry points that evaluate a statement from inside a
function), not a category of work the VM cannot express.

## Decision

**The VM executes every statement.** Planning produces a `PhysicalPlan`, lowering produces a
program, and the interpreter runs it. There is no other executor.

**The per-record engine and the `compute-only` strategy are removed.** The `legacy/` module, the
per-record parts of `doc/` and `dbs/` (`dbs/iterator.rs`, `dbs/processor.rs`, `dbs/group.rs`), and
the index planner that served them (`idx/planner/`) are deleted. Nothing in `exec/` calls into
`crate::legacy`. A schemaless or untyped record is not a reason to keep a second executor: the VM
runs it as `Value`s through the instructions that need no declared kind.

**The operator tree's operators survive only as host services and as a test oracle.** An operator
the VM's host calls (a scan over a storage range, a full-text or vector index iterator, a graph
traversal, a record fetch) is a service the program requests through a host step. The operator
tree is no longer an executor: nothing compiles a statement to a tree of `ExecOperator`s and runs
it outside a test build, and the statement operators (`LET`, `RETURN`, `FOR`, blocks, `INFO`,
`SLEEP`, schema statements, bare expressions) and the schema statements' own executors are
removed. The compiler that builds a tree from a bound plan (`Planner::plan_logical`, the fold and
the SELECT and write planning beneath it) and the operators only it reached are compiled under
`cfg(test)`, where the parity tests run a plan on both engines and compare the rows.

**A statement the VM cannot run is an error, never a fallback.** A statement or a block the VM does
not lower fails with an error that names the construct. `Decline::{Gap, Routing}` collapses to the
single case that remains: every decline is a gap, and a gap is a failure. The VM's refusal of a
decline, which `SURREAL_VM_STRICT=declines` measured, is the default once the remaining gaps close
and is not a setting. The two routes that would otherwise recover from a gap, the operator tree and
the per-record engine, no longer exist.

**The switches go.** `SURREAL_EXEC_VM`, `SURREAL_VM_STRICT` and `SURREAL_PLANNER_STRICT` are
removed: there is one engine and no decline to tolerate. `SURREAL_PLANNER_STRATEGY` and
`--planner-strategy` are accepted and ignored, with a deprecation warning that names the removal,
so a deployment that sets it keeps starting. They are removed in 4.0. The language-test runner's
`planner-strategy` axis is removed with the strategies it enumerated.

## Consequences

- **The language suite runs each test once.** No second engine is an oracle, so a divergence
  between engines can no longer fail a test. The expectation is the reference: a `.surql` file's
  recorded results are the contract, and a change to one is reviewed as a change to the language.
  Differential confidence comes from the expectations and from the VM's own unit tests, which state
  each instruction per row and check the vectorised form against it (ADR 0004's amendment,
  `doc/executor/RFC_EXPRESSION_BYTECODE.md`).
- **The upgrade tests are the guard for stored clause text.** A stored clause is the SurrealQL text
  of its definition, and the only check that an old binary's text still lowers and runs on the
  current VM is `language-tests/tests/upgrade/`. That suite is the reason `ToSql` stays faithful to
  the clause (`CLAUDE.md`, "Keep the upgrade tests current"), and it takes over the role the
  per-record engine had as the engine that read any stored text.
- **A gap is visible.** A statement that reaches a construct the VM does not lower fails the
  statement where a decline once answered correctly and silently. A new construct is not shippable
  until the VM lowers it, and the language test that exercises it fails until then.
- **The operator tree is a host service and a test oracle, and nothing else.** A non-test build has
  no route from a statement to a tree: a scalar subquery and a function body are bound and planned
  as programs (`vm::plan_checked`), and every closure invocation runs the body as a unit lowered
  for the invoking frame and `VERSION` stamp (`vm/closure.rs`): no closure body is evaluated as a
  `PhysicalExpr` tree. A body the host lowers no unit of fails each invocation with the reason or
  the error compiling it reports. The tree
  compiler and the scans, joins, layers and graph operators only it ran are the reference the
  parity tests (`exec/vm/tests_scan_parity.rs`, `tests_plain_data.rs`, `tests_enforced.rs`,
  `tests_front_end.rs`) compare the VM against, and the planner's and the operators' unit tests
  drive them directly. They leave the tree when what those tests pin is stated as recorded
  expectations.
- **The `EXPLAIN` text has one source.** It renders from the plan the VM runs, not from a tree that
  might not be the executor. The VM's physical plan is what `EXPLAIN` shows, and the tests that
  pinned the operator tree's plan text are rewritten against it.
- **Performance work has one target.** `doc/executor/VM_REGRESSIONS.md` records the shapes where
  the VM is slower than the operator tree it replaces and the cause of each. A regression is fixed
  in the VM, with no executor to route the shape to instead.
- **ADR 0004 is Accepted.** Its evidence list (per-batch async overhead, per-row cost at a fixed
  representation, resumability under budgets) is not a gate any longer; those questions are
  answered by the VM's benchmarks, not by a choice between executors.
- **ADR 0005 is superseded.** Its body stays as the record of why the per-record engine was kept
  while the streaming engine was incomplete.
