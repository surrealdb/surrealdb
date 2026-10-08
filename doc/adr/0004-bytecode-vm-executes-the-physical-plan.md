# ADR 0004: A bytecode VM executes the physical plan

**Status:** Accepted (ADR 0007) · **Scope:** `surrealdb/core/src/exec/vm/`, `surrealdb/core/src/exec/operators/`

## Context

ADR 0003 splits planning into a stateless logical plan and a catalog-bound physical plan. This ADR
decides what executes the physical plan.

ADR 0002 (execution engine direction) decides the opposite of this ADR: the plan layer stays a
push-based tree of `ExecOperator`s exchanging `ValueBatch`es, there is no plan-level bytecode VM,
and flat synchronous programs appear only inside the expression layer, for pure subtrees. It
rejects a synchronous executor rewrite outright, on the grounds that async at the operator
boundary is paid once per batch.

This ADR diverges from ADR 0002, and ADR 0007 supersedes ADR 0002's position. ADR 0002's objections are answered one by one below, because
each is a real constraint, and the ones that hold are kept as requirements on the VM.

## Decision

The physical plan is compiled to a program of dense, fixed-width instructions over slots and run by
a synchronous interpreter loop, with no allocation in steady state. Async exists only at the seam
between the interpreter and its host — storage reads and suspension points — never inside the loop.
The program operates on batches of typed columns with selection masks, not on rows of `Value`.

The instruction set is split in two, and the split is part of the ISA, not discovered later:

- **Control** — loops, subroutines, segment cuts, budget charges. Scalar, CPU-resident.
- **Bulk** — pure functions from typed buffers and a mask to typed buffers. These are the unit that
  may be vectorised or moved off the CPU. Every bulk opcode has a scalar implementation, so
  correctness never depends on the hardware present.

The design is `doc/executor/BYTECODE_VM_DESIGN.md`.

**Amendment (expressions as bytecode).** `doc/executor/RFC_EXPRESSION_BYTECODE.md` refines how the
two halves appear in the program. Every instruction is specified per row, so the laws and the
differential tests are stated once; the VM runs a program's pure regions over a vector of rows with
a selection vector, which is the bulk half, and an instruction with an effect, a per-row loop or a
nested program ends a region and runs row at a time, which is the control half. The split is a
property of regions the compiler forms, not two instruction sets. A fallible instruction in a region
defers its failure, and the lowest failing row's error is raised, which keeps the errors the
row-at-a-time order raises. Storage reads are batched primitives the host serves (`Scan`,
`GetMany`, `Count`), issued once per vector.

## ADR 0002's objections

**"Flattening the plan to bytecode erases the per-node metadata the optimizer and the write-path
barrier rules operate on."** It does not, because the program is not what the rules operate on. The
physical plan (ADR 0003) is the optimisation substrate and keeps every per-node property — access
mode, ordering, partitioning, cardinality. The ADR 0001 barrier rules are checked on the physical
plan, before it is compiled. Bytecode is a compilation target, the way machine code is for a
compiler that optimises over an IR. **Kept as a requirement:** no rule may need to read a program;
if one does, it is written against the physical plan instead.

**"The operator layer is the convergence point for SurrealQL, GQL and the Postgres dialect;
per-dialect bytecode emission is not."** Agreed, and nothing here emits bytecode per dialect. Every
dialect lowers to the logical plan. There is one compiler, from the physical plan to the program.
**Kept as a requirement:** the compiler takes the physical plan and nothing dialect-specific.

**"Plan-level VMs are single-threaded by construction, row-at-a-time."** A VM instance is. The
engine is not one instance. Data parallelism is partitioning in the physical plan: each partition
runs its own program instance, and the VM's dispatcher owns the degree of fan-out, adapting it at
segment cuts. Row-at-a-time does not apply: the bulk opcodes are defined over batches.

**"Streams are built before they are polled, so re-entrant execution is cheap; a VM would
re-implement that for no gain."** Recursion and lookups become subroutine calls in the program, and
suspension becomes a segment cut with an explicit program counter. That is a reimplementation.
What it gains is the next point.

**"Async at the operator boundary is control plane, amortised per batch."** This is the disagreement
that decides the ADR, and it is an empirical one. The surrealedge experiments behind
`BYTECODE_VM_DESIGN.md` measured async and coroutine recursion in the execution core at a +257%
throughput regression, and fused synchronous kernels at 2.6× Apache Arrow's throughput. Neither
was measured inside this engine, and ADR 0002's own probe measured per-row async at 11.8× a
synchronous loop without measuring per-batch async on its own.

## Questions the measurements answer

These are the measurements that bear on the VM's design. Each is taken in this engine, in release
mode, and recorded beside ADR 0002's probes. Acceptance (ADR 0007) does not wait on them; they
direct the VM's work.

1. **Per-batch async overhead, on its own.** The cost of the operator boundary once the expression
   layer has ADR 0002's synchronous kernel path.
2. **Where the time goes per row after (1).** The workstream's measurement of roughly 3µs per row
   attributed the cost to decode and allocation. A VM does not change decode on its own; a VM that
   does not reduce the per-row cost at a fixed representation is justified by what it enables
   (resumability, budgets, offload), and its speed is held to the shapes recorded in
   `doc/executor/VM_REGRESSIONS.md`.
3. **Resumability and fairness.** Whether cooperative suspension with bounded budgets is expressed
   by the program's segment cuts at acceptable cost.

## Consequences

- The VM is the one executor. It consumes the physical plan (ADR 0003), and ADR 0007 removes the
  operator tree as an executor and the per-record engine with it.
- The operator tree's operators remain as host services the program calls: storage scans, index
  iterators, graph traversal, record fetches. A rule that needs an executor's behaviour reads the
  physical plan, never a program.
- A statement the VM cannot lower is an error, so the VM's coverage is a correctness property, and
  the language suite's expectations are the reference its output is checked against.
