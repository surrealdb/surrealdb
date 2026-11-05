# ADR 0006: A clock read answers the statement's instant

**Status:** Accepted · **Scope:** every read of the clock (`EffectSet::CLOCK`) and every place that folds,
plans or caches an expression that holds one —
`surrealdb/core/src/ctx/context.rs`, `surrealdb/core/src/dbs/executor.rs`,
`surrealdb/core/src/exec/function/builtin/time.rs`, `surrealdb/core/src/exec/planner/util/literals.rs`,
`surrealdb/core/src/exec/index/access_path.rs`

## Context

`time::now()` reads the wall clock, so two calls can answer differently. Whether one statement reads
it once or many times is observable: `CREATE |t:50| SET at = time::now()` writes one timestamp or up
to fifty, and `SELECT * FROM t WHERE at <= time::now()` compares a record stamped by an earlier
statement of the same request with an instant that has moved on.

A plan that holds the instant it was made at is wrong for every run after the first. A program the
compile cache keeps, a statement planned when its request arrives and run later in the request, and
a function body compiled once and called from many statements each outlive the moment they were
planned.

## Decision

1. **A clock read answers one instant per top-level statement.** `time::now()`, `time::timezone()`
   and the `time::*` accessors that default an omitted datetime to the current time
   (`time::year()`, `time::nano()`, ...) all read it, in a projection, a data clause, a condition, a
   permission predicate or a stored clause alike. Two reads in one statement agree whatever row,
   iteration or nested frame they run in, and a `SLEEP` inside the statement does not advance the
   instant. Two top-level statements of one request read two instants.
2. **The instant lives on the `Context`.** The executor installs it with
   `Context::set_statement_clock` once per top-level statement, immediately before the statement
   runs. Every context derived from it copies it: a `FOR` body, a block member, a subquery, a
   function or closure body, triggered code, `eval::surql` and `eval::gql`, and a script's
   `surrealdb.query()` therefore read the clock of the top-level statement that runs them and install
   none of their own. `statement_clock` (`builtin/time.rs`) reads it, and takes the wall clock at
   the call only where no statement installed one. An `ASYNC` event body is a statement of its own,
   so it takes its own instant.
3. **A script reads the same instant.** `surrealdb.functions.time.*` answers `now` and `timezone`
   from the context's instant and substitutes it for an omitted or `NONE` datetime argument of the
   accessors that default one. The set of names is the registry's `EffectSet::CLOCK`, never a list
   kept beside it.
4. **No fold evaluates a clock read, and no plan holds an instant.** `try_fold_to_literal` folds only
   calls the registry declares deterministic, which a clock read is not, so a plan holds the call
   and a program the compile cache serves reads the clock of the statement that runs it. The fold
   host refuses any value that reads the clock (`NotFoldable::Clock`).
5. **A condition that compares with the clock is still an index range.** The index analyzer reads
   `time::now()` and `time::now() +/- <duration literal>` as a bound that reads the clock
   (`BoundOperand::StatementClock`), and the scan resolves it from the context's instant when it
   opens. `EXPLAIN` prints the bound as the call, never as an instant. The bound is offered only to
   a caller allowed to call `time::now()`, because the scan reads the clock without a
   function-name check of its own: a caller denied the function keeps the comparison as a filter,
   which raises the denial when it is evaluated. Two bounds of one side, one of which reads the
   clock, are not compared before the scan opens, so neither is taken as the tighter and no range
   holding one is proved empty. A bound over a datetime component of the record id is not offered,
   since widening it to whole seconds needs the instant.
6. **Entropy and external calls stay per call.** `rand::*` and generated record ids vary per row by
   design, and an `http::*` call is observed once per call. Only the clock has a statement-scoped
   answer, because it is the statement's time.

## Consequences

- A statement is never planned again because it read the clock.
- Both clock positions are pinned by tests: `language/functions/time/now_evaluated_per_call.surql`
  (one instant for every row of a statement) and `reproductions/one_statement_reads_one_clock.surql`
  (one instant across a loop body, a subquery, a function body, a closure, a nested query and a
  script, and two instants across two statements).
- `every_clock_builtin_reads_the_context` (`builtin/time.rs`) pins that each `EffectSet::CLOCK`
  builtin reads the context and refuses the context-free call.
