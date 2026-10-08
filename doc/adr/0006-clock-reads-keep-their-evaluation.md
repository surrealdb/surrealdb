# ADR 0006: A clock read keeps the evaluation the language gives it

**Status:** Accepted · **Scope:** `time::now()` wherever it is folded or planned —
`surrealdb/core/src/exec/logical/fold.rs`, `surrealdb/core/src/exec/planner/util/literals.rs`

## Context

`time::now()` reads the wall clock, so two calls can answer differently. Whether a statement over
many rows reads it once or once per row is observable: `CREATE |t:50| SET at = time::now()` writes
either one timestamp or up to fifty.

The language has an answer, and it is the per-record engine's: **every call is evaluated where it
stands, once per row**. The streaming engine keeps that answer everywhere but one place: in a
condition it folds `time::now()` to one value for the statement, because `at > time::now() - 1d`
becomes an index range only once its right side is a literal. That is a deliberate choice — the
statement's time, as SQL's `NOW()` is — and the streaming engine has made it since it was
released. It is observable, but only by a condition that reads the clock more finely than the
statement takes to run; a time-window comparison does not.

A fold that reaches every expression — a projection, a data clause — collapses per-row values the
language promises into one, and that is observable. Neither engine may do that.

The cost of per-row evaluation is not a reason to fold. `Utc::now()` reads `CLOCK_REALTIME` through
the vDSO on Linux: no system call, no lock, and independent on every thread, so it scales with the
threads reading it. On a host whose clock source is not the TSC it falls back to a system call,
which is slower but still correct. What parallel evaluation does change is order, not cost: rows
evaluated on different threads read the clock in no particular order, and two rows can read the
same instant. The language has never promised that values are distinct or increasing — clock
resolution and clock adjustment already break both — so per-call evaluation keeps no promise that
parallelism would break.

## Decision

1. **In a projection, a data clause, a permission predicate, a mutation's `WHERE`, and every other
   position that is not a condition, `time::now()` is evaluated per call, per row.** It is never
   folded there. A rewrite that duplicates or removes a clock read is removing or adding an
   evaluation, and is held to the evaluation contract like any other.
2. **In a `WHERE` condition, `time::now()` is one value for the statement.** It is the only position
   where the streaming engine may fold it, because the fold is what lets an index answer the
   condition.
3. **Only `time::now()` has a statement-scoped exception, because it is the statement's time.** The
   exception is not a property of the `CLOCK` effect. The `time::*` accessors that read the clock
   when given no datetime (`time::nano()`, `time::year()`) read it again on every call, in a
   condition too: `WHERE time::nano() % 2 = 0` is a fresh bit per evaluation, and
   `reproductions/defaulting_time_reads_the_clock.surql` pins that a subquery holding one stays a
   subquery. Nothing else that is non-deterministic folds in any position: entropy's per-row
   variation is the point (`WHERE rand::float() < 0.1` samples each row), and an external call, a
   counter or a wait is observed once per call. The position is generic (`ExprContext::condition`);
   the exception it grants is named.
4. **A clock value is never baked into a plan that outlives the statement.** Under ADR 0003 plans
   are cached. A condition's single value is taken when the statement starts and held in a slot the
   plan reads, not written into the plan as a literal. Folding it at plan time is correct only while
   the plan is used once.
5. **Both positions are pinned by tests**, and no rule may change what either one answers:
   - `language/functions/time/now_evaluated_per_call.surql`, on every planner strategy: a
     projection, `SET` and `INSERT` read the clock per row.
   - `exec::logical::fold` unit tests: a `WHERE` folds the call and a projection in the same
     statement does not; a data clause keeps the call; `rand::float()` stays a call in a `WHERE`.

## Consequences

- The streaming engine's fold of `time::now()` is gated on its position being a condition
  (`fold::is_condition`). The per-record engine is unchanged.
- Before the physical plan cache lands, a condition's clock fold becomes a statement-start value.
  Until it does, a cached plan would serve a stale timestamp, so caching and this change land
  together.
- The same reasoning applies to every call whose answer depends on when it runs. Entropy
  (`rand::*`, generated record ids) is per call, per row, and never folded in any position. An
  external call (`http::*`) is evaluated exactly where and as often as written, in every position,
  because the number of calls is itself observable.
