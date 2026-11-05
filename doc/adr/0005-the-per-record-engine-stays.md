# ADR 0005: The per-record engine stays

**Status:** Superseded by ADR 0007 · **Scope:** `surrealdb/core/src/dbs/`, `surrealdb/core/src/doc/`, the engine
choice in `surrealdb/core/src/exec/`

Superseded by `0007-one-engine-the-bytecode-vm.md`: the per-record engine and the `compute-only`
strategy are removed. The body below is the historical record.

## Context

Two engines answer SurrealQL: the per-record engine (`dbs/` and `doc/`, driven by `dbs::Iterator`)
and the streaming engine (`exec/`). Every statement reaches one of them through a routing choice,
and the language suite runs every test through both and fails on any divergence.

`doc/executor/AST_TO_EXECUTION.md` treats the routes into the per-record engine as gaps. Closing
all of them is what makes removing that engine possible, and its `compute-only` pin is removed with
the engine. This ADR decides that the engine is not removed.

## Decision

**The per-record engine is permanent.** It is the right executor where the streaming engine's
advantages have nothing to work with: schemaless and untyped data, where there is no declared kind
to unbox against, no column to vectorise over and no layout to skip by.

**The streaming engine is total.** It can express every statement, and no statement goes to the
per-record engine because the streaming engine lacks a form for it. Routing to the per-record engine
is a choice the physical plan makes and records, visible in `EXPLAIN`, never the only thing the
engine can do.

**A decline is one of two things, and the code says which.** A *gap* is a statement the streaming
engine should run and cannot yet; it is a bug. A *routing decision* is a statement the per-record
engine is the better executor for; it is correct. `Decline::{Gap, Routing}` records the
distinction at every site.

## Consequences

- `SURREAL_PLANNER_STRICT=1` refuses a gap and permits a routing decision. It measures coverage; it
  does not define correctness. **Strict is not a CI gate**, because a gate would fail on correct
  routing. The gap count is tracked, and each new gap is reviewed.
- A guard that declines a shape the streaming engine can now express is a gap and is removed. A
  guard written before a capability existed goes on declining after it arrives, and nothing fails,
  because the per-record engine answers correctly. Review asks of every decline which kind it is.
- The differential suite stays: every language test runs on both engines, and the per-record engine
  keeps its place as the reference answer for the shapes it serves.
- The `compute-only` pin stays with the engine, as the way to force a statement onto it.
