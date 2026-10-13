# Blocks on the VM

A block, `LET`, `FOR`, `IF`, `THROW`, `RETURN`, `BREAK`, `CONTINUE` and `SLEEP` are control flow in
the physical plan (`Plan::Control { control }`, the [`Control`] tree in `vm/plan.rs`, built by `vm/map.rs`) and
lower into one program (`vm/lower_block.rs`, its block instructions serviced by `vm/block.rs`).

The VM is the one executor (`doc/adr/0007-one-engine-the-bytecode-vm.md`): a block it cannot lower
fails when the query arrives, with an error that names the construct, and nothing runs it
another way.

## The control tree

| node | holds |
|---|---|
| `Seq` | the statements in turn; a `LET` binds its name for those after it |
| `Let { name, kind, value }` | the value's node, coerced to `kind` |
| `For { param, range, body }` | the range's node, evaluated once, and the body's node |
| `If { branches, otherwise }` | each branch's condition node and branch node, and the `ELSE` node |
| `Throw { value }` | the value's node, raised as the statement's error |
| `Return { value }` | an expression (`Value`) or a statement's node |
| `Break`, `Continue` | the innermost loop's exit and next element |
| `Sleep { duration }` | a written duration |
| `Value(expr)` | an expression holding no statement, evaluated in the control pipeline |
| `Statement { plan, source, changes_catalog, planned_over, modelled, analysis, late_bound, raises_when_reached }` | a statement's physical plan, compiled with the program, the catalog it was mapped against, for `EXPLAIN ANALYZE` of it what it renders, whether it is planned late-bound (see *Statements planned late-bound*), and whether its plan is a refusal raised when the block reaches it |

An `IF` in a statement's place is control flow: its conditions are evaluated in turn and the first
truthy one's branch runs, its statements statements of the block in a frame of their own, so a
`BREAK`, `CONTINUE` or `RETURN` inside is the block's jump, and its value is the branch's (`NONE`
when none runs). An `IF` used as a value — a `LET`'s, a `RETURN`'s — is an expression. A binding
that folded an `IF`'s condition leaves the block it chose, which is the block. The block's own
expressions — a `FOR` range, an `IF` condition, a `RETURN` or `THROW` value, a `LET` of one —
are `Value`s when they hold no statement, and expression statements otherwise, so the statements
they hold are compiled with the program.

A statement is bound and mapped when the block is: binding holds the loop parameter and each
earlier `LET` as bound around it (`Planner::shadowing`), never folding their values, so one plan
serves every reach. The statements as written sit beside the plan in `PhysicalPlan::sources`, each
with the names bound around it, for what the lowering reads of a statement's footprint; the tree
holds no statement.

A block is compiled in full when the query arrives, and running it plans, binds and lowers
nothing. A schema statement is compiled with the program like any other statement. Where the block
can know its effect, it is run speculatively while the block is planned and the statements after it
are planned against what it defines (see *Planning against the block's own schema statements*);
where it cannot, and a statement the block runs after it reads what it may change, the block is
planned again with the statements after the change late-bound (see *Statements planned
late-bound*).

Every statement is a group of the program. A statement the VM does not bind, map or lower as a
group fails the whole block. A statement after the block's first schema statement that the planner
refuses raises the refusal when the block reaches it. A statement that reads the clock is bound like any other: no binding folds a clock read, and every read answers the instant of the top-level statement that holds the block. `EXPLAIN ANALYZE` of a statement maps the statement it explains,
and the text it renders of that statement's plan is fixed when the query arrives
(`Program::analyses`). A semi or anti join the logical rules lifted out of a filter is mapped as the
membership predicate it was lifted from (`map::memberships_of`), whose subquery is a statement
nested in the filter.

The program's table dispatch (`vm::dispatch`) reads each table as the catalog held it when the
query arrived, as a value written in `FROM` is bound. A value in `FROM` only the data
names, and a `FETCH`, resolve as they run, against the catalog the block's schema
statements leave: a block whose schema statements define, alter or remove a table such a read may
reach, or one of its fields, is planned with what follows them late-bound. A table the dispatch holds no unit for is one the
run defined by writing to it, read by its key range (`nested::undispatched_table`).

A statement after a schema statement, or one nesting a statement, a closure or a function call, is
compiled with the program like any other.

## Statements planned late-bound

A block whose plan cannot know what a schema statement leaves for the statements after it — one in
an `IF` branch or a loop body that is not peeled, one after a change the plan was not made knowing,
one whose speculative run did not end, a statement reaching code that may run one, a schema
statement inside another statement's expression, and every change to what the session's namespace
and database resolve to (`DEFINE` or `REMOVE NAMESPACE` or `DATABASE`) — is planned again with what
follows such a statement late-bound (`Planner::binding_late_after_schema`, `map::Blocks::late_after`).
Such a block runs no schema statement speculatively. It maps statements in order until one may
change the catalog or the names; every statement after it in its block is planned late-bound
(`Planner::late_binding`), and so is all of a loop body one of whose statements may change either,
since a later iteration runs after the change. Nothing is planned once the program runs.

A late-bound statement is bound with no catalog: no table source is resolved, so a read applies the
table's permissions, computed fields and field permissions as the catalog stands when it runs
(`descriptor::field_state`, kept per table version); a parameter is read when it runs, and a
`time::now()` in a condition is evaluated then. Each catalog object it names is loaded by name when
it runs, from the compile cache by the version the transaction reads, and compiled in a fault where
the cache holds none:

- a write names its stages, as a write that may reach any table does (`StageTables::by_name`), and
  runs each record through the bundle of the statement's stages for the record's table
  (`fault::serve_stage_bundle`), keyed by the statement, the table's catalog version and the build,
  with one compilation per frame, for the version the record's write meets. Its write contexts are
  checked against the catalog before they are taken again (`WriteScope::revalidating`), and a write
  over a table or a record names it as a target, gated as the write runs;
- a call of a user-defined function runs the function's call unit (`vm::call_unit`), the call
  compiled for the definition the transaction reads, which makes the call's checks, binds and
  coerces its arguments and runs the body;
- a parameter whose `PERMISSIONS` clause is not the one the program lowered is judged by the
  parameter's unit (`vm::param_unit`), the clause compiled and kept by its hash;
- a schema statement checks the definition against the catalog it finds when it runs, as one whose
  name is evaluated does; `DEFINE TABLE` and `REMOVE TABLE` judge the table they meet when they run,
  whatever planned them (a view's source tables, a LIGHTWEIGHT relation's edges, the fields of a
  table changing its type).

A late-bound statement resolves the session's namespace and database again, at the level it
declares, before it runs (`BlockInstr::RunEmbedded`'s `resolve`), since a statement before it may
have removed or replaced either. A program holding one declares that it may write anything the
catalog lets it, and a loop holding one keeps no loop invariant over what it writes.

## Constants and loop invariants

A `LET` of a literal with no declared kind binds a constant: the statements and control
expressions in its scope read it as written, where nothing inside them binds the name again and
outside closure bodies, which read the name where they are called. The binding itself is kept, so
what the program does not lower reads the name as before. An `IF` condition the constants settle
decides its branch when the block is mapped, and a branch it rules out is never compiled.

A statement nested in a loop body's statement whose value is kept for one run of that statement
(`Strategy::Once`) is kept for the loop instead (`NestedSpec::hoist`) when it reads no name the
loop binds and nothing in the body may write: no write or schema statement, no custom function or
closure, and no compiled expression or subquery that writes. Its value is forgotten when the loop starts
(`IterStart`), and it runs where an iteration first reaches it.

## Planning against the block's own schema statements

The executor's planner for a statement it is about to run plans a block against what the block's
own schema statements write (`vm/speculate.rs`). Each schema statement is mapped and optimized as
any other, then its program is run over a speculation's view of the transaction
(`kvs::Transaction::speculate`), a transaction of its own: its writes land in an overlay, its reads
of the current state merge the overlay over the transaction's, and it has a cache and count of
catalog stamps of its own. The overlay each run leaves is the catalog the statements after it are
bound, mapped and lowered against, through a planner over a view from it (`speculate::view`).
Nothing a speculative run writes reaches the storage; the statement's real run, when the block
reaches it, writes the same definitions, being the same program over the same catalog.

Only what is handed a view reads through its overlay. Every other holder of the transaction — a
task an earlier statement left running, a concurrent caller of a client's transaction — reads and
writes the transaction as before, so a speculation needs nothing else to stop using the
transaction. A view takes no write once its speculation has ended, so a task of a speculative run
still holding it reaches neither the overlay nor the storage. What is planned over a view holds no
transaction and runs over the real one.

A speculative run takes the statement's reads and writes of the key-value view and nothing else:
what acts on the node — its caches and services, an index build or a view's backfill, an action at
commit — is left for the real run (`store::speculative_skip`). What the run draws is drawn once and
reserved: an id leased from a sequence (a table's, an index's), a uuid a definition records, and the
catalog version the statement stamps (`StoreInstr::NextId`, `NewUuid` and `StampCatalog`, each with
its `draw` and `reserved`). The program's run of the same draw takes it (`Program::reservations`), so
the catalog later statements were planned over is the catalog the real run leaves, versions
included.

A clause a schema statement evaluates — a parameter's value, a sequence's start — that a second
evaluation may answer differently (the clock, entropy, the world outside, the rows it reads) or that
acts outside the transaction is evaluated by the statement's real run alone
(`StoreInstr::RealRunOnly`): its speculative run raises, so the statement is not modelled and a
block reading what it changes after it fails. A secret a definition generates — an access method's key —
is generated when the statement is built, so both runs store it alike.

A schema statement's effect is known when:

- it runs once for each run of the block, in the order the block holds it — not in an `IF` branch
  or a loop body (but see peeling below);
- every statement before it changes the catalog only as the program knows: each earlier schema
  statement was modelled, and no statement's footprint writes the catalog (a write defining its
  table, one through a table with events, a function body, a value invoked);
- nothing before it that the block was not planned knowing writes what it reads: an index's build
  or a view's backfill after a write to its table is not known;
- its speculative run ends.

The plan's premise is the catalog the transaction read when it was planned, which only the
program itself can change, and then only as it was planned knowing: other transactions' changes
are not visible to it, and a conflict with them aborts at commit.

What a statement reads and writes is its **footprint** (`exec/logical/footprint.rs`): writes by
table, from the statement and what the catalog says it triggers — a table's events run what no
footprint names, a table views are defined over has its views' tables written, a delete clears
references in any table, a write defines a table the catalog does not hold — and catalog entries for
schema statements; reads conservatively, by table where the statement names it, and of every table
where a path, lookup, method or fetch may follow a record link, or where a table's select permission
or fields evaluate a body. A rewrite is legal only when what the statements it moves or folds past
write overlaps nothing the moved or folded statement reads or writes (`Footprint::conflicts`).

With a schema statement's effect known, the plan folds what follows from it:

- A statement whose speculative run wrote nothing, left nothing for its real run, leased no id and
  answered `NONE` — `IF NOT EXISTS` over what exists, `IF EXISTS` over what does not — is not
  compiled: the block holds `NONE` in its place. Its caller's permission to run it held, since the
  run passed its checks. An `OVERWRITE` writes its definition and stamps the table whatever it
  finds, so it is never folded this way.
- A statement whose speculative run raised a refusal it declares (`StoreInstr::Raise`, `Allow`,
  `RefuseNoWrite`) — a definition that exists or is missing, a capability the caller lacks — raises
  it when the block reaches it, which ends the block: nothing after it is compiled. The statement
  itself still runs and raises.
- A loop whose schema statements change the catalog on its first iteration and do nothing after it
  is **peeled** (`Control::For`'s `first`): the first element runs a body planned as the block's
  own statements are, and the rest a body planned against the catalog the first leaves, its schema
  statements folded away. It is peeled only where the later body changes the catalog only as the
  program knows and writes nothing its folded statements read. What follows the loop is planned
  against the catalog the first iteration leaves, which a loop that runs no iteration leaves
  undone: unless its range is known to hold an element, a block reading what the first iteration
  defines after the loop fails.
- A write whose evaluation cannot change the catalog while it runs lowers no event stage for a table
  with no events and no index stage for one with no indexes (`write_catalog_fixed`).

A read nested in a loop body's statement is kept across the loop's iterations (`NestedSpec::hoist`)
when it reads no name the loop binds and nothing in the body writes what it reads, by footprint;
code the program does not see keeps it per run.

A top-level query's statements are planned one at a time as the executor reaches them, each against
the catalog the ones before it left; the block's folds apply within a block.

What is not known makes the block planned with what follows it late-bound when a statement it
runs reads what the change may write (`lower_block::data_dependent_ddl`): a schema statement in a
branch or a loop body that is not peeled, one after a change the program was not planned knowing,
one whose name is evaluated or that the VM does not lower, and code a statement reaches that may
run one. `DEFINE NAMESPACE`, `DEFINE DATABASE` and their `REMOVE` change what every name resolves
to, and the statements after one in a block are late-bound, so that the namespace and database they
run in are resolved when the block reaches them.

## One program

The control flow is the program's own instructions in one pipeline:

| instruction | does |
|---|---|
| `RunEmbedded { statement, collect, on_break, on_continue }` | run a statement's group of the program's pipelines over the program's state; gather its value as the operator it stands in for does |
| `RunAnalyzed { statement, analysis, collect }` | run a statement's group as `RunEmbedded` does, counting and timing its rows, and answer the rendering of `Program::analyses[analysis]` over what was measured; a signal the group raises ends it and is not raised |
| `Bind`, `CoerceKind` | bind a `LET` or loop parameter in its slot, and in the current frame when something may read it there; coerce to `LET`'s kind |
| `Save`, `Restore` | scope a loop iteration's bindings, and a block's inside a block |
| `IterStart`, `IterNext` | a loop's array elements or range integers, taken lazily, one slot per loop |
| `Interrupt`, `Sleep` | timeout and cancellation checks at each iteration; `SLEEP` |
| `Return` | leave with `RETURN`'s value |

A loop is `IterNext` at its head and a jump back to it. `BREAK` and `CONTINUE` are jumps to the
loop's exit and to its next element, restoring on the way the frames the blocks inside the loop
saved; a `BREAK` or `CONTINUE` a statement inside the loop signals lands on the same targets (a pad
restoring the frames first when it is inside a block within the body). A `BREAK` or `CONTINUE`
with no loop of the program around it is `Signal`, raising the signal for the program's caller.

`Collect` reproduces each site's gathering: a block keeps its last statement's value, `RETURN` one
value, `NONE` or an array (a `RETURN` inside appending its value), a `LET` its first value (a
`RETURN` inside answering it), and a loop body nothing.

## Statements

Each `Statement` lowers into the program as a **group** of its own pipelines (`Program::groups`,
`Pipeline::group`): lowering it shares the program's tables — constants, fields, paths,
expressions, limits, sorts, splits, aggregates, permission frames, host pipelines, subqueries,
calls — so its instructions index them directly, and a statement that does not lower leaves them
as they were. `RunEmbedded` runs the group where the block reaches it, in the frame the block has
then, over the program's own `State` (`host::run_group`); nothing is planned or checked on the
way. What a reach starts afresh is the statement's slice of that state: its paging (evaluated in
the frame), sorts, aggregates and permission frames, and its write, which runs on the one
`WriteState` the statement keeps, reset per reach. What that write resolves from the catalog is
kept for the run: each table's definitions its write contexts read (`WriteScope::kept`,
`WriteCtx::rebound`), which a reach takes with its own frame and its own compiled clauses, what its
gate has answered of each table, and the stage programs lowered for a table known only as records
arrive. The block's row-level state is set aside while
the statement runs. The statement's rows are gathered as a stream's reader would gather them: once
a signal or an error ends it, only the rows it had flushed.

A statement's tail is the group's: `VERSION` a host scalar evaluated first, whose stamp the
group's sources read at; `TIMEOUT` a host scalar evaluated in that frame, a deadline the host
enforces around the group's run and checks when it ends; `ONLY` the group's cardinality check,
answering its one row, `NONE`, or failing past one. The value takes the shape of the
tail's contract: `ONLY` a row, `TIMEOUT` rows. A tail clause that holds a statement is not lowered,
and the block holding it fails.

`LET` and `FOR` names are bound in **slots** of the state (`Bind`'s `slot`), and every read of one
the program lowers itself — in the control pipeline or in a statement's group — is `LoadSlot`,
resolved at lowering to the binding it reads. A name is bound in the frame too (`Bind`'s `frame`)
only when something in its scope may read the frame's parameters: a compiled expression, a host
pipeline (which runs over a state of its own), a permission frame, a write (whose stages and
clauses read the frame), or a source that evaluates an expression.

What lowering read of the catalog holds for the whole run: other transactions' changes are not
visible to it, and a conflict with them aborts the transaction at commit, like any other. The
program changes the catalog in two places only, both known when it is lowered: a schema statement,
and a write that defines the table it writes to. Every statement after either was lowered knowing
what it changes — the schema statement's speculative run, and the open table no definition holds
that a write's table binds as — or reads nothing it may change, and a block for which neither holds
fails when it arrives. So no catalog change of the program's own makes what it compiled
stale, and nothing is compiled again while it runs. A **commit premise** is a different thing: what
a write assumes about the data (a key absent, a version unchanged), checked at commit, whose failure
aborts the transaction.

A statement is planned in the planner the block was planned with — the principal and its
permission frame, the `VERSION` and search scopes, the function path, the parameters an enclosing
construct binds, and the re-entry depth — with the names the blocks around it bind shadowed.

## What the program declares

The executor resolves the namespace and database from a statement's context level before it runs
it, and picks the transaction's mode from its access mode. A block program declares the levels and
modes of the plans of its statements; one holding a statement after a schema statement requires a
database and writes. A late-bound statement declares the level its plan needs, which its
`RunEmbedded` resolves the session's names to again before it runs.

## Not yet

What still runs apart from the one program and its one state:

- A statement in a host pipeline — a paging clause, a write's data clause or `RETURN`, a graph
  hop's `WHERE`, an aggregate's argument — is a program of its own: a host pipeline runs over a
  fresh state of its own, which holds neither the running program nor its slots.
- A write loop still binds its names in the frame each iteration, and a write's instructions read
  them there: its Rust stages, its compiled clauses, its event and cascade statements (programs of
  their own) and every stored clause they evaluate read the frame, and a record's image frames
  (`$value`, `$before`, `$after`, …) may shadow a name, so a slot read is sound only for a write
  none of whose parts reads the frame, which lowering does not yet establish. A reach still
  compiles the write's clauses (its contexts' compiled expressions, views and permissions) and
  builds its permission frames afresh.

[`Control`]: ../../surrealdb/core/src/exec/vm/plan.rs
