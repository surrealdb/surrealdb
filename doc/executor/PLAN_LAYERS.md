# Plan layers: what each layer owns

In short: **the logical plan is *what*, the physical plan is *where*, bytecode is *how*.** Precisely:
the logical plan decides **what** is computed (meaning, legality, effects); the physical plan decides
**where and by which method** (access paths and indexes, placement — morsels, workers, leaf hosts —
batching and prefetch, and the algorithms: hash vs lookup join, top-k vs full sort, partitioned vs
merged grouping, guarded specialisations as prepared alternatives); bytecode spells out **how**,
mechanically (typed, guarded or generic instructions, offsets, registers) and chooses nothing.

A query passes through four layers. Each one owns a distinct kind of decision, and each may only
narrow what the layer above allows:

```
logical plan   → represents what needs to be done, from the query and the catalog definitions it
                 reaches, and states what is legal
physical plan  → chooses how it is done: how data is accessed and moved, within what is legal
lowering       → emits bytecode for the chosen plan; decides nothing
VM             → executes; adapts only inside what the physical plan allowed
```

Planning and execution do not overlap. Planning reads definitions and never data, and produces a
program whose every step has a self-contained meaning. Execution runs that program as written: it
never re-plans or recompiles its own code, never re-decides an access path, placement or plan
shape, and its only freedom is scheduling inside a decision (morsel width, chunk sizes, giving
places back, the merge strategy of a partitioned aggregate). A program may load a version-keyed
unit it does not hold (a function body, a table's read, a write's stage bundle, a closure body); a
load that misses the compile cache compiles that unit, which is code of its own, not a change to
the running program. Query text handed over at run time (`eval::surql`, `eval::gql`, an embedded
script's `surrealdb.query()`) is compiled as a nested query: a program of its own, in the caller's
transaction. Speculation over a block's schema statements and the late-bound re-plan after them
happen while the request is planned, before its program runs.

Results, error messages and error order are the same whatever any layer chooses. A layer that
cannot prove a choice keeps those answers chooses the serial, unrewritten form.

## Logical plan: what is computed

The logical plan owns the meaning of the query.

- **Legality and effects.** Every expression has an effect summary (`EffectSet`), every statement
  a footprint, and sibling statements dependence edges. From these the logical plan states, per
  pipeline, what may be split, how partial results combine, which paging is present and which
  error wins (`logical::pipelines`). It states legality; it never chooses placement.
- **Semantic catalog.** The logical plan may read catalog *definitions*: function bodies,
  `COMPUTED`/`VALUE`/`ASSERT` expressions, params, permissions, views, events and reference
  actions. Definitions are read at the catalog version the plan is bound at; a plan never outlives
  its snapshot.
- **Precise resolution.** A definition's effect summary is resolved from its body, including what
  a write triggers on the tables it writes (events, field clauses, view maintenance, reference
  actions), transitively. A summary is **unknown** — the top of the effect lattice, which refuses
  splitting, overlap and reordering and counts as a possible catalog change — only for:
  - bodies the planner cannot read as SurrealQL: JavaScript, modules, silos, models,
    `api::invoke`, external calls;
  - closures passed by value whose origin is not visible in the query;
  - cycles in the definition graph, and asynchronous events;
  - definitions reached after a possible catalog change point in the request.
- **Per-row regions** (target; see "Implemented and target"). Scan bodies, filters, projections, loop bodies and closures invoked per
  element are regions over a row. A value that depends on nothing of the row and whose effects
  allow it is hoisted out (evaluated at first use, memoised), a condition of that kind unswitches
  into precompiled alternatives that each raise the source's errors, and code whose value nothing
  reads is dropped only when its effects are reads at most. A value the principal cannot see is
  never computed.
- **Permissions** (target; see "Implemented and target"). Table and field permissions are conjuncts tagged with their authority frame and
  the row they read (the user's filter reads the field-masked row, the table permission the real
  row). They may form one conjunction with the user's filter, which the physical plan may reorder
  and serve from an index, only when no write of the statement reaches what the permission reads,
  and a filter conjunct goes before a permission conjunct only if it is pure and cannot raise.
- **Rewrites.** Reordering and pushdown within the dependence edges; operator fusion that is never
  worse for any input (adjacent filters merged in order, `LIMIT` over `ORDER BY` as top-k,
  projection pushdown, hoisting row-free pure expressions). A fused filter keeps its predicate an
  ordered conjunction and a projection keeps its field list, so the physical plan can still match
  conjuncts to indexes. Filters that may raise are never reordered.
- **Inlining and common subexpressions, decision included.** The logical plan inlines a
  definition when the catalog-inlining rule allows it (code that runs in the request's first
  transaction, before any possible catalog change); everywhere else it calls the definition's
  version-keyed unit. It shares a pure subexpression that is more than a field read or a constant,
  and recomputes the rest. Two expressions are one only if they also agree on their authority frame
  (permissions enforced or suppressed, `AUTH LIMIT`, the principal class) and snapshot; a re-bound
  name is a new value; sharing never crosses a statement. A read of data is the same value twice
  only within a statement in which no write — triggered code included — reaches it.
- **Loops.** A loop whose iterable is known when the query is planned (a literal, a `LET`
  constant, a pulled-in definition) may be unrolled. A loop over a run-time value is never
  compiled during execution; its iterations' independence (`ForeachLegality`, in the IR one
  `loop_legality` per loop site) is stated for the physical plan to use.
- **Stream multiplicity and keyed footprints.** Write
  footprints name the keys they touch along the loop element (`KeyOf::Elem`), and every loop source
  carries a multiplicity fact: `Distinct` for table, index and range sources and for `set` values
  (a type fact or a literal set); `MayRepeat` for arrays — literal, `$param`, subquery results held
  as values — unless deduplicated. A write loop is independent only over a `Distinct` source whose
  iterations write disjoint keys; a `MayRepeat` write loop stays serial. Loops with an ordered exit
  (`BREAK`, a `RETURN` leaving the loop, `any`/`all`/`find`) stay serial.
- **Write-target provenance.** A write whose target is a loop
  variable (`FOR $u IN (SELECT * FROM user) { UPDATE $u … }`) is static when the type analysis
  (`analysis/types.rs`) pins the variable's `id` to one table over a `Distinct` scan; otherwise the
  target is dynamic, its effects unknown, and the loop serial. This is a logical fact; the physical
  plan only reads it.
- **The statement clock.** `time::now()` and
  every CLOCK read is one value per top-level statement, nested queries included: a per-execution
  operand of the program, evaluated once at statement start. The logical plan treats it as a
  statement constant (hoistable, CSE-able) and never folds it into a plan literal, so a cached
  program reads a fresh value on every run.
- **Statement independence.** `independent` states that two statements could run in either order.
  `BEGIN`, `COMMIT`, `CANCEL`, `USE`, `RETURN`, `THROW`, `SLEEP`, schema statements and any statement
  with effects beyond reading are barriers; a `LET` orders only against statements that read or bind
  the same name (its footprint is per name), and one whose value has effects beyond reading is a
  barrier. Statements never run concurrently, and writes are never merged across statements.
  Reads may be batched across statements (see "Reading early").

### Type facts and type hints

A declared type is a **fact** the logical plan may propagate only where storage guarantees every
record conforms: a table in the strict `ROW` layout whose records are all at layout generations
the catalog describes (new writes are encoded against the declared kinds; `MIGRATE` rewrites
older records). The fact is catalog state, pinned to the plan's catalog version, and does not
hold while a migration of that table is in progress (until then the facts are the union of the
live generations' types). Everywhere else a declared type is a **hint**: a plan may specialise on
it only behind a check of the actual value, with the generic form as the fallback. Function
parameter types are facts at any layout, since a call checks them.

Facts let the logical plan resolve record links to one table, treat non-`option` fields as never
NONE, drop type-based failures from `may_raise`, and let the physical plan and lowering use typed
instructions over fixed offsets. A dynamic target (a write to a record held in a variable, a
`type::table` source, a link) is planned exactly when a fact pins it to one table, through a
checked specialisation when a hint does, and through the generic form otherwise; its effects are
the union of its possible tables' effects, or unknown where those cannot be enumerated.

### Definitions are inlined; data is read

Only catalog definitions are inlined. A record is data: no layer folds a record's value into a
plan, and nothing reads data while a query is planned (a plan depends on the catalog version, a
record can change under an unchanged catalog). For a read such as
`(SELECT VALUE aah FROM user:drew)` inside a statement:

- the logical plan may rewrite it to an equivalent form (keeping its meaning exactly: an array of
  one value, or an empty array when the record does not exist) and states that it reads no row of
  the outer statement and has no effects, so it may be hoisted and evaluated once per statement
  (memoized for the run, not folded into the plan) and independently of other reads;
- the physical plan chooses the access: a point read by key, once per statement, batched with
  the statement's other point reads into one multi-get;
- the VM performs the read.

### Hoisted expressions run at first use

An expression hoisted out of the per-row loop — a permission conjunct that reads nothing of the
row, a read that does not depend on the row — is evaluated at the first row that reaches it, not
when the statement starts, and its value is reused for the rest of the run. A statement whose
rows never reach it never evaluates it, so it raises no error the unhoisted form would not. Under
morsels the memoised value is a result, error included, and each row that reaches it raises that
error at its own input position. A read is hoisted only when no write of the statement reaches it.

Reading early is a step of the program with its own meaning, not a rule the VM applies: the
physical plan may emit a prefetch of point reads (one batched read that warms them, raises nothing
and produces no value), within a statement or across statements of the request, for reads no earlier write of the request
reaches; the read at first use
is an ordinary read, served from the prefetch when present, that raises its own error at its own
position.

### One owner per kind of redundancy

- **Repeated computation** — the same pure expression in several places — is shared by the
  logical plan (common-subexpression sharing).
- **Repeated data reads** — the same record read more than once — are not merged by the logical
  plan. The physical plan batches point reads, within a statement or across statements, and reads
  each key once per batch,
  which also covers reads written differently that reach the same key. It may treat two reads of
  a key as one only where the logical plan's footprints show no write to that record between
  them.

## Physical plan: how data is accessed and moved

The physical plan owns the physical catalog — indexes, record layout, statistics — and every
access decision:

- access path: table scan, index, range bounds — a bound may be the statement timestamp, a
  per-execution operand (`WHERE at > time::now() - 1d` on an indexed `at`) — and the residual
  filter;
- a `Choose` over different access paths only when the `ORDER BY` keys end in a unique key or the
  record id, so tied keys never pick different rows under a `LIMIT`; otherwise one fixed plan;
- sort strategy, top-k, spilling;
- join algorithm (lookup, hash, nested loop);
- aggregation strategy;
- batching reads: collecting keys and issuing one multi-get, when iterations are independent and
  also inside a serial loop, for reads whose footprint is disjoint from every write of the
  statement (triggered writes included); a key the statement writes is never batched. A loop's
  batched read is one bytecode instruction fused across the loop's key ranges, not one per key; it
  raises nothing and produces no value, and each consumer reads at its own position, served from
  the batch when present, raising its own error;
- placement and parallelism: through one `Policy`, `Serial` or `Morsels { input, combine, paged }`
  per pipeline, stored on the plan;
- pipeline fusion: dropping morsel tracking where splitting does not pay.

- planning every nested unit (subquery, called body, cascade) before lowering, with the enclosing
  statement's facts (target; until then lowering plans them with the enclosing statement's facts
  passed in).

The physical plan never states legality and does no effect analysis. It may pick any form the
logical plan allows or a weaker one along `TopK ≥ StableMerge ≥ Concat in key order ≥ Serial`
(`Aggregate` and `Distinct` weaken only to `Serial`) — never a stronger one. A physical rule that
removes a breaker (an index delivering the order a sort asked for) restates the pipeline's legality through the logical plan's fuse rule; it never edits legality
itself. It refuses for physical reasons only: the
access path, the transaction mode, the input being too small to split.

All placement decisions go through `Policy`. Its inputs are the logical legality, the physical
facts of the pipeline and, at run time, `Signals` (rows and bytes visited, cost per row, places
held). Statistics are an input of the same `Policy`; until they exist, the decisions use run-time
signals and fixed thresholds. Statistics, when they exist, are a versioned physical-catalog epoch:
a plan records the epoch it used like any other catalog version.

A pipeline placed on another host (a leaf) may require the unit-call form of a definition instead
of its inlined form, to keep the shipped code small. That is a placement choice and is the only
way the physical plan affects inlining.

## Lowering: emits

Lowering turns the physical plan into bytecode. It copies each pipeline's decision; a pipeline
the physical plan did not place is an internal error. Lowering has no optimisers, plans no nested
unit and derives no legality or placement; it lowers every form (a form without a fast path
becomes instructions or a unit call), so no statement is declined (the last three are targets;
see "Implemented and target"). Pipelines lowering creates for itself are serial by construction
and listed as such. Lowering checks a decision and may only narrow it: a combine stronger than
the legality the decision was made against, and a program that writes holding a `Morsels` decision,
are internal errors; a decision made from a physical fact that the code lowering emitted
contradicts (a paging the merge cannot replay, a record fetched by name, a row rebound, another
count of distinct sets, a nested statement or block the fact did not count) becomes `Serial`.
Whether a placed body runs inside the storage cursor or over decoded rows follows from the code it
emits; that never changes the decision the program carries, and changes how the VM runs it (see
below).

## VM: executes and adapts within the decision

The VM never re-plans or recompiles a running program's code; it loads the units the program
names (see above). For a `Morsels` decision it:

- starts on the statement's own task and splits the remaining range only once the scan proves
  large;
- widens by letting idle workers steal part of a running morsel's range, and re-splits large
  morsels;
- narrows when other statements wait: a worker gives its place back after a quantum of chunks and
  parks its remainder for the coordinator or a later worker;
- merges results so they equal the serial run: concatenation in key order, `START`/`LIMIT` replay,
  sorted runs and top-k, partial aggregates, merge-side `DISTINCT`, first error by input position.

For a fanned-out write loop (`UPDATE`/`DELETE`/`UPSERT`/`INSERT`, and loops whose body writes) the
VM runs the compute part of each iteration on workers and applies the candidate writes on the
coordinator in key order — store, indexes and unique checks, events, references, views,
notifications — so results, error order and what triggered code observes equal the serial run
(parallel compute, ordered apply). Bytes held only because of fan-out are a revocable lease: under
memory pressure the coordinator takes them back and the rest runs in serial order, so a query never
fails because it fanned out.

A `Serial` decision runs serially. The VM never turns `Serial` into morsels. The decision names
where a morsel's rows come from only as far as the plan can know: a table scan or a record range
is `Range`, every other source that splits is `Batches`. A body that cannot run in the storage
cursor reads decoded rows instead, and a body that runs there over a source the coordinator hands
on runs serially. The VM narrows a decision by the emitted body this way and never widens it,
through one function (`morsel::effective_input`) that `RFC_MORSEL_PARALLELISM.md` (§7.2) states
as a table.

## Definition reads

The logical plan is built from the query and every catalog definition it reaches. Planning reads
those definitions from an in-memory catalog cached by catalog version, so planning a query reads
no catalog keys while the catalog is unchanged.

The cache is filled by range reads, never by per-name lookups:

- the database-level definition ranges (`!tb` tables and their permissions, `!fn` functions,
  `!pa` params, and the other definition kinds), read by kind, skipping the state keys that sort
  among them (`!cf`, `!cv`, `!ov`, sequence and table-id state);
- for each table, the contiguous definition range under its prefix, `!ev` through `!ix` (events,
  fields, the views it feeds, its index ids and index definitions — no record data, which sorts
  elsewhere under the same prefix).

For a typical schema the whole catalog loads in two batched reads after each catalog change. A
schema with very many tables loads its table ranges lazily, per table a query reaches, still
cached by version. Per-table catalog versions (`!cv`) let a change invalidate only the tables it
touched. Index definitions arrive with their table; the logical plan does not use them and the
physical plan reads them from the same cache, so neither layer reads catalog keys of its own.
The contiguity of `!ev` through `!ix` is part of the keyspace's contract: no data-carrying key may
sort inside that range.

## Implemented and target

Implemented: logical legality keyed by source, with breaker segments, `Legality::fuse` and the
statement's refusal; effect summaries of stored functions and `COMPUTED` fields, resolved from
their bodies at the plan's catalog version, with a call to a stored function still refusing as an
unseen body even when its summary is pure; statement barriers for `independent` (nothing executes
on it); the physical plan's placement pass, `Policy` and decisions stored on source nodes;
lowering that copies them and checks them as above; the nested statements of a plan placed in
their host's scope; morsel execution with stealing, narrowing and the merges above.

Target design, not yet implemented: one IR of operations with SSA values and nested regions in
three dialects (expression, logical, physical), with layering enforced by conversion targets and a
verifier that runs in release at every phase boundary; per-row-region optimisation (hoisting,
unswitching, dead-code removal, field liveness); a program cache keyed by query shape whose
premises (catalog objects and versions) are checked once before a run; permission
conjuncts merged with filters; values hidden from the principal never computed; lowering that
covers every form and plans no nested unit (it hands the units it compiles into the current
program to the physical planner today, and a unit a running program loads from the version-keyed
cache is placed in a scope of its own); `in_place` as an expression property the physical plan
reads, in place of a fact of the emitted code that narrows a decision when the pipeline runs;
inlining and common-subexpression sharing of pulled-in
definitions in the logical plan (the per-statement value-sharing DAG is still built in lowering);
loop unrolling; batched reads for loop iterations (independent loops and serial loops alike, under
the statement-wide footprint rule above); the version-keyed catalog cache filled
by range reads; triggered code represented as logical plan nodes rather than effect summaries;
statistics as a `Policy` input; placement on leaf hosts; type facts from strict layouts and
dynamic targets bounded by them (a dynamic write target is planned generically with unknown
effects today; a read through a record link, a graph edge or a dynamic source is planned with the
effects of the tables the statement names, so the `COMPUTED` fields of the records it reaches are
not folded); stream multiplicity, keyed write footprints and write-target provenance for write loops;
the statement clock as one operand per statement; resolving a `DELETE`'s reference actions, a
body's schema statements, a write whose target the body does not name and a `MATCH` plan in a body
precisely instead of as unknown; stored functions whose summary is pure becoming eligible for
splitting; the logical `may_raise` taking type facts into account (today an allow-list that counts
every call, arithmetic, cast, dereference and catalog-sourced expression as raising).
