# RFC: One bytecode runtime, with durable triggered programs

**Status:** draft. Proposes that everything a statement causes runs as bytecode
on one runtime. A statement's program *calls* the work that must be atomic with
it and *triggers* the work that follows it. A triggered program is durable:
its state lives in the KV store, any node resumes it when its owner stalls or
dies, and its external effects run inside recorded boundaries. Timers and
recurring jobs are triggered programs that fire later. Every program declares,
from its instructions, what it reads, what it writes and whether it can fail.

Companion: [`RFC_RECORD_LAYOUT_AND_MIGRATE.md`](RFC_RECORD_LAYOUT_AND_MIGRATE.md).
The write path it builds on: `doc/adr/0001-streaming-write-path.md`.

---

## 1. Why

What a write causes today is spread across the write stages
(`core/src/exec/operators/write/stage.rs`), and a queue for async events drained after commit with its own
retries (`core/src/doc/event.rs`). Background work — index builds, compaction,
data migrations — runs under per-type task leases (`datastore/src/tasklease.rs`),
each with its own resume logic. One runtime gives all of it one execution
model, one declaration of effects and one failure contract.

## 2. Call and trigger

A program starts another in one of two ways.

| | runs | commits | fails |
|---|---|---|---|
| **call** | inside the caller's transaction, before the caller continues | with the caller, or not at all | as the caller fails |
| **trigger** | independently, on any node, in transactions of its own | on its own | by its own contract (§6) |

A trigger records an *invocation* in the caller's transaction and returns at
once. The invocation exists exactly when the caller commits: an acknowledged
write always has its triggered work recorded, and a rolled-back one never
does.

What is called and what is triggered is decided by what a reader or a later
statement can observe. A unique violation fails the write that causes it; the
statement after a write sees its index entries; a sync event's writes are the
statement's writes. Index maintenance, unique checks, sync events, table views
and cascades are therefore *called*. Async events, migration backfills, index
builds and any derived structure permitted to lag are *triggered*.

Calling the write's derived work as subprograms also lets it batch: a program
over a batch of written records computes every index entry the batch needs and
writes them together. The order entries are written in is not observable; the
order records' stages run in is, and stays record by record (ADR 0001, R3).

## 3. What a program declares

Every program carries a header derived from its instructions. The lowering
computes it; a runtime loading a program — from the catalog, an invocation or
another node — derives it again from the code and refuses the program when
the two differ.

| declaration | derived from |
|---|---|
| **effects** — the union of `EffectSet` over its instructions | each opcode's fixed effect; a call's builtin's declared set |
| **writes** — none; within the transaction, with the tables written; external | write, host-call and call instructions |
| **reads** — the row only; tables and key extents; the catalog; the session | field, scan, fetch and host-call instructions |
| **fallibility** — infallible, or may raise | every node, judged by kind (companion RFC §5.2) |
| **determinism** | the clock, entropy, external reads, advancing state |
| **triggers** — the programs it may trigger | trigger instructions |
| **boundaries** — the external effects it performs, each in a boundary | boundary instructions (§5) |
| **ISA version** | the encoding; a runtime refuses a version it does not speak |

Each consumer accepts a program by a predicate on the header:

| consumer | accepts |
|---|---|
| a migration transform | pure, reads the row only, infallible, deterministic, triggers nothing |
| a row program inside the storage cursor | no host calls |
| a fan-out leg on another node or a Worker | writes nothing, reads a snapshot, no host call the leaf cannot fulfil |
| a called stage | writes only within the caller's transaction, no external effect |
| a triggered program | any, with every external effect inside a boundary |

## 4. A triggered program's state

### 4.1 In the KV store

An invocation is a record in its own key band:

- the program, by the content hash of its bytecode, stored once;
- its inputs;
- its checkpoint: the instruction it resumes at, its registers, the cursor position of any scan
  it is in, and the values it has read from the clock or drawn from entropy since it started;
- the results of the boundaries it has completed (§5);
- its owner: a node id, an **epoch**, and the lease's expiry;
- attempt and takeover counts.

A second key band indexes invocations by lease expiry, so finding stalled work
is a range scan, not a sweep of every invocation.

### 4.2 Checkpoints

A program checkpoints where the VM yields: between the batches of a scan,
before a boundary, at a timer. A checkpoint writes the program's state in the
same transaction as the writes it has made since the last one, so the KV store
always holds a state consistent with every write it has committed. A resumed
program starts at its checkpoint and repeats nothing that committed.

Registers are written as owned values at a checkpoint. A row program's
registers borrow from a row and never live across one, so a program never
checkpoints inside a row.

Values from the clock and entropy are inputs the checkpoint records: a resumed
program sees the time and the random values it saw before, and so reaches the
same decisions.

### 4.3 Ownership and takeover

- **The owner renews** its lease at half its duration, the renewal the task leases already use
  (`Valid → Renewable → Expired`).
- **Any node scans** the expiry index at intervals with jitter. An invocation whose lease has
  expired, or whose owner the node registry reports dead, is eligible.
- **Takeover is one transaction:** read the invocation, confirm it is still eligible, write the new
  owner with the epoch incremented. Two nodes racing both read the same record; the transaction
  store lets one commit, and the other retries or moves on.
- **Every checkpoint is fenced.** The checkpoint's transaction reads the invocation's owner and
  commits only if the epoch is still its own. A previous owner that was paused rather than dead —
  a long pause, a partition — cannot commit once it has been taken over: its checkpoint
  transaction fails, and it stops.

Correctness therefore does not depend on clocks. Clock skew between nodes
changes only how soon stalled work is taken over.

### 4.4 Work that keeps failing

A program that raises is retried with backoff. A program whose owner keeps
dying — one that crashes the node running it — shows as a rising takeover
count. Either, past its limit, moves the invocation to a dead-letter band,
which `INFO` reports and an operator can retry or drop.

## 5. Boundaries around external effects

A write through the transaction happens exactly once, because it commits with
the checkpoint. An external effect — an `http::` call, a write to a bucket, a
model call with a cost — happens outside the transaction, and a program resumed
after a crash could perform it again. Every such call in a triggered program
runs inside a **boundary**:

1. **Intent.** Before the call, a checkpoint records that step *k* is about to run, with an
   idempotency key derived from the invocation id and *k*.
2. **Call.** The call runs, carrying the key where the effect supports one (an HTTP header).
3. **Result.** A checkpoint records step *k*'s result.

On resume:

- **A recorded result** is used as the call's answer. The call is not made again.
- **An intent without a result** is the one window no protocol closes: the call may or may not
  have reached the endpoint. The step runs again with the same idempotency key, so an endpoint that
  honours the key performs it once. Such an effect is at least once, and the program's
  documentation says so.

A boundary is explicit in the program. A triggered program with an external
call outside a boundary is refused when it is compiled. Whether the SurrealQL
surface writes boundaries explicitly or the lowering places one around every
external call is a language decision; the runtime requires only that every one
exists.

## 6. Timers and recurring jobs

- **A timer** is an invocation with a fire-at time, in a key band ordered by it. Nodes claim due
  timers under the same ownership rules as any invocation (§4.3).
- **Waiting.** A triggered program that waits — for a time, a duration — checkpoints, arms a
  timer, and releases its lease. It resumes, on whichever node claims the timer, at its
  checkpoint. A program can so run for days without holding a node.
- **A recurring job** is a timer that re-arms itself in the transaction that commits the firing,
  so a crash neither loses a tick nor doubles one. The firing time is the invocation's clock input.
  Its SurrealQL surface — something like `DEFINE JOB … EVERY 1d DO { … }` — is a language decision.

## 7. Where programs run

The program is the unit that moves: the same bytecode runs in the caller's
transaction, as an invocation on any node, or on a leaf a statement fans out
to. What a place can run is what its host fulfils — a Worker has no transaction
to write through, so it runs only programs that write nothing — and the header
is what that is checked against before the program is sent.

## 8. Plan changes

- A physical `Write` node lowers its stages to called programs, and the plan records each
  program's header; the planner uses the write footprints to decide what may share a batch.
- An event, view or index definition compiles to a program when it is defined, stored by content
  hash, and is invalidated with the definition's catalog version. An `ASYNC` event is triggered; a
  sync one is called.
- `MIGRATE` lowers its transform to a program that satisfies the migration predicate, and triggers
  its backfill (companion RFC §6).

## 9. Order of work

1. The program header, derived and re-derived, on the VM's existing programs.
2. Invocations, checkpoints and takeover, with the migration backfill as the first triggered
   program: it is new, replaces nothing, and exercises checkpointing, fencing and resume.
3. Boundaries, with async events as triggered programs, replacing their queue.
4. Timers and waiting.
5. Called stages, starting with index maintenance, in agreement with the write path's owner and
   ADR 0001, each checked against the language suite.

## 10. Open questions

1. The key bands for invocations, the expiry index, timers and dead letters, and how completed
   invocations are compacted.
2. Fairness: how running invocations shares a node with foreground statements, and per-tenant
   quotas.
3. Transaction size limits (TiKV) for batched called work, and how a batch splits when it would
   exceed them.
4. Whether the existing per-type task leases (`tasklease.rs`) become invocations, or stay as they
   are for the work they already run.
5. How an operator observes invocations in flight, retries, takeovers and dead letters.
