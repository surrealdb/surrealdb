# ADR 0007: One engine, the bytecode VM

**Status:** Accepted · **Scope:** `surrealdb/core/src/exec/`, `surrealdb/core/src/dbs/executor.rs`,
the engine and strategy switches of the server and the language-test runner

**Supersedes:** ADR 0005 (the per-record engine stays), and ADR 0002's position that there is no
plan-level VM.

## Context

SurrealQL has to be answered by one engine or by several. The candidates are a per-record
evaluator (the document pipeline in `doc/` driven by `dbs::Iterator`, which evaluates an
`expr::Expr` one record at a time), a push-based tree of `ExecOperator`s exchanging `ValueBatch`es
compiled from a bound `LogicalOp`, and the bytecode VM (`exec/vm/`), which maps the bound plan to an
inert `PhysicalPlan`, lowers it to a program and runs it on a synchronous interpreter under an
async host (ADR 0004).

Running more than one means every semantic rule the language has lives in each executor and the
copies must agree. Agreement is checked by a differential suite that runs every test under every
engine and strategy, which is also the largest part of the suite's run time, and a decline routes a
statement to an executor nobody chose. ADR 0002 recorded a position in which the plan layer stays a
tree of operators with no plan-level bytecode VM, and ADR 0005 held the per-record engine
permanent. Both keep several executors.

The VM covers the language. The statements it does not lower are a short list of gaps (nested
schema statements, `USE` arguments, live queries, the stored-API path, closures and the host entry
points that evaluate a statement from inside a function), not a category of work the VM cannot
express.

## Decision

**The VM executes every statement.** Planning produces a `PhysicalPlan`, lowering produces a
program, and the interpreter runs it. There is no other executor.

**There is no per-record engine.** Nothing in `exec/` calls an evaluator that walks an `expr::Expr`
one record at a time, and no strategy selects one. A schemaless or untyped record is not a reason to
keep a second executor: the VM runs it as `Value`s through the instructions that need no declared
kind.

**Operators are host services, not an executor.** An operator the VM's host calls (a scan over a
storage range, a full-text or vector index iterator, a graph traversal, a record fetch) is a service
the program requests through a host step. Nothing compiles a statement to a tree of `ExecOperator`s
and runs it. Statement kinds (`LET`, `RETURN`, `FOR`, blocks, `INFO`, `SLEEP`, schema statements,
bare expressions) are program instructions or host steps, not operators. A stored expression is a
`Clause`, whose evaluator is a VM unit. `ExecOperator` is the description of a plan that `EXPLAIN`
renders and the host runs.

**A statement the VM cannot run is an error, never a fallback.** A statement or a block the VM does
not lower fails with an error that names the construct. Every decline is a gap, and a gap is a
failure. The engine has no setting that tolerates a decline.

**The engine has no switches.** One engine leaves no decline to tolerate and nothing to select.
`SURREAL_PLANNER_STRATEGY` and `--planner-strategy` are accepted and ignored, with a deprecation
warning that names their removal in 4.0, so a deployment that sets one keeps starting. The
language-test runner has no `planner-strategy` axis.

## Consequences

- **The language suite runs each test once.** The expectation is the
  reference: a `.surql` file's recorded results are the contract, and a change to one is reviewed as
  a change to the language. Differential confidence comes from the expectations and from the VM's
  own unit tests, which state each instruction per row and check the vectorised form against it
  (ADR 0004's amendment, `doc/executor/RFC_EXPRESSION_BYTECODE.md`).
- **The upgrade tests are the guard for stored clause text.** A stored clause is the SurrealQL text
  of its definition, and the only check that an old binary's text still lowers and runs on the
  current VM is `language-tests/tests/upgrade/`. That suite is the reason `ToSql` stays faithful to
  the clause (`CLAUDE.md`, "Keep the upgrade tests current").
- **A gap is visible.** A statement that reaches a construct the VM does not lower fails where it
  is run. A new construct is not shippable until the VM lowers it, and the language test that
  exercises it fails until then.
- **No statement has a route to a tree of operators.** A scalar subquery and a function body are
  bound and planned as programs (`vm::plan_checked`), and every closure invocation runs the body as
  a unit lowered for the invoking frame and `VERSION` stamp (`vm/closure.rs`). A body the host
  lowers no unit of fails each invocation with the reason or the error compiling it reports.
- **The `EXPLAIN` text has one source.** It renders from the physical plan the VM runs, so the text
  describes the executor and no other structure.
- **Performance work has one target.** `doc/executor/VM_REGRESSIONS.md` records the shapes where
  the VM is slowest and the cause of each. A regression is fixed in the VM, with no executor to
  route the shape to instead.
- **ADR 0004 is Accepted.** Its evidence list (per-batch async overhead, per-row cost at a fixed
  representation, resumability under budgets) is settled by the VM's benchmarks, not by a choice
  between executors.
- **ADR 0005 is superseded.** Its body stays as the record of why the per-record engine was kept
  while the streaming engine was incomplete.
