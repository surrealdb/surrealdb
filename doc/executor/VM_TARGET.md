# The VM's target: one abstract plan, one program

What the executor is converging on, and every place it does not yet. A change
that moves the VM closer ticks its row here; a row leaves when nothing of it
remains.

## Routing

The VM is the one executor (`doc/adr/0007-one-engine-the-bytecode-vm.md`). A
statement it cannot lower is an error that names the construct; no other
engine takes it. Below, a statement or block the VM "declines" or "does not
lower" is a gap that fails the statement until the row that names it closes.
The operators the host serves a program with (storage scans, index iterators,
record fetches) are host services and not a second executor.

## The model

- **The physical plan is abstract.** Its nodes are dataflow and control:
  sources, filters, projections, sorts, aggregates, write effects, and the
  control nodes `Seq`, `Let`, `If`, `Loop`, `Return`, `Break`, `Continue`,
  `Throw`. No node holds a statement, a logical plan, or a clause to be
  planned later.
- **Lowering produces one program per statement.** A block is that program's
  control flow: a `FOR` is a loop whose variable is a register or a frame
  slot and whose iteration is a jump; a `LET` binds a slot; a nested statement
  is a pipeline or subroutine of the same program; `RETURN`, `BREAK` and
  `CONTINUE` are jumps carrying their signals.
- **Nothing is planned while the program runs.** A query's program, and
  everything it can reach — the body of every function its calls reach, by
  static name and transitively, recursion included; every closure literal it
  makes and every closure a parameter holds when it is planned; every branch
  outcome — is compiled once, when the query is planned. A running program
  binds, maps, lowers and compiles nothing; a call whose body the plan cannot
  compile fails the statement, decided when it is planned. The exceptions are explicit: `eval::surql`, and plan invalidation
  (below). Compiled function bodies are kept by the node as units that later
  plans link (see *Compiled units*). What lowering read of the
  catalog — tables, fields, indexes, permissions, views, events — holds until
  the program itself changes the catalog; other transactions' changes are
  isolated from it and conflict at commit. At each point where the program may
  change the catalog — a schema statement, a write that defines its table —
  it checks whether it did, and **invalidates** what it compiled: that code is
  compiled again against the catalog as it is now, and the program continues.
  The common path pays nothing. (A **commit premise** — what a write assumes
  about the data — is a different thing: checked at commit, it aborts.) A
  table known only as records arrive gets a specialisation lowered to bytecode
  for it.
- **Everything a statement does is bytecode, and the host fulfils effects.**
  Storage reads and writes, key encoding and the record codec are
  instructions; the host performs a write on the coordinator and records an
  intent on a fan. The only native code is the host's effects and the index
  kinds' statically dispatched trait (full-text, vector, …) — never a
  statement body or a stage body.

## The layers

1. **Logical: the whole program, control flow included.** A statement or a
   block is one bound logical plan: `Sequence`, `Let`, `Foreach`, `Return`,
   and `If` whose branches hold statements. `LET` and `FOR` names are scoped
   parameters, never folded while binding. Rules work across statements:
   constant folding and dead code through `LET`, loop-invariant hoisting,
   `FOR` over a `SELECT` rewritten to a set-based pipeline, lookups inside a
   loop merged into one read, a `LET` value choosing a later statement's index.
2. **Physical: the same control flow, over abstract nodes.** `Seq`, `Let`, `If`,
   `Loop`, `Return`, `Break`, `Continue`, `Throw` hold physical children;
   writes and DDL are effect nodes. The physical rules run over the whole tree:
   access paths from statistics and indexes, a sort's strategy (top-k, in
   memory, spilled, range-partitioned), regions and fan-out — a loop whose
   body's effects allow it included.
3. **Bytecode: one program.** A control pipeline runs the data pipelines as
   subroutines; a loop is a jump; a `LET` is a register or a frame slot;
   signals are jumps.
   A nested statement is part of the same program. Whether it stays nested is
   the logical planner's decision (decorrelation into semi-, anti- and
   group-joins; hoisting what reads nothing of the outer row; merging a
   subquery in `FROM` into its parent). How what stays nested runs is the
   physical rules', from statistics and effects: **once**, as a pipeline run
   first into a slot, for what does not depend on the row; **per-row apply**,
   as a subroutine the outer pipeline calls with the correlated values in
   registers, inlined when small — the only choice when the nested statement
   writes; a **hash join**, an inner pipeline building a sink the outer
   pipeline probes, for many outer rows and no index, its build leasing from
   the memory pool and partitioning to spill when refused; an **index nested
   loop**, a per-row subroutine over an index on the correlated key. Lowering
   implements the node it is given and decides nothing.
4. **The catalog changing mid-program.** After an effect that can change the
   catalog, the program checks whether it did; if so, what it compiled is
   invalidated and compiled again from its source against the catalog as it is
   now, resuming from the program's explicit state. Binding against the
   program's own static DDL is the refinement that avoids the recompile.

## Distribution

- **The plan marks where it may be cut, and nothing more.** A cut point is a
  structural fact: a scan may split by key range, a group repartitions by the
  hash of its key, a sort by ranges of its key, a gather must keep order. Each
  fragment between cut points lowers to bytecode whose output is a `Send` to a
  partition and whose input a `Receive`, effects the host fulfils.
- **Whether to cut, and how wide, is decided while the program runs, per
  morsel.** From the work left (morsels remaining times their measured cost),
  what else the node or the platform is running, and what the query may spend
  (CPU time, subrequests, fan-time, storage operations). Width changes at morsel
  boundaries. A cut point not taken collapses to a hand-off inside one program,
  so one fan and a hundred run the same fragments.
- **Destinations are partitions, not instances**, so instances may join, leave
  or fail while senders are unaware; a morsel is deterministic (a key range at a
  version), so a lost partition is rebuilt by sending its morsels again.
- **A batch splits when its segment is row-local.** Computed fields, arithmetic,
  array operations, a field's `DEFAULT`/`VALUE`/`ASSERT`, permission guards and
  a record's whole write compute from the row and snapshot reads alone; the
  only state across rows is in sinks that combine — output rows by morsel
  position, sort runs, aggregate partials, write intents. A write's statement-
  level state combines too: the rows `ONLY` counts are summed and judged, the
  matches an `UPSERT … WHERE` over a table counts are joined, so its guaranteed
  record is written by the commit fragment only when no morsel matched, and the
  records affected are summed. A record's write fans only when nothing else in
  the statement reads what it writes, the reads of its table's computed fields,
  defaults and guards included.
- **A limit changes the strategy, never the answer.** Less to spend means fewer
  fans and more time; less memory means spilling and more passes.

## Compiled units

A user-defined function's body, compiled for a call, is a unit the node keeps
(`vm::units`, bounded by `ExecConfig::compiled_unit_cache_size`, least
recently used evicted first) and later plans link instead of compiling the
body again. Lookup and compilation happen only while a query is planned.

- **Key**: namespace and database (names and ids), the function's name, the
  principal the body is bound for (the caller's, capped by the definition's
  `AUTH LIMIT`, so the owner and each record user are apart) and whether
  permissions are enforced, the re-entry depth, the names bound around the
  call and the capabilities.
- **Dependencies**: the transaction records every catalog entry a
  compilation reads (`datastore::catalog_reads`); the unit keeps the state of
  each (a table's catalog version, or its definition's content hash when it
  was never stamped; a function's definition; a `DEFINE PARAM`; a database;
  for a compilation that listed a database's tables, the tables it defines
  and every catalog version stamped in it, read in two scans once per
  transaction) and a lookup serves it only while every one reads the same
  (`exec::catalog_deps`). A schema change invalidates exactly the units whose
  dependencies it touched. A recording naming what it cannot identify keeps
  no unit. Where the datastore is the store's sole writer (memory, RocksDB,
  SurrealKV), it counts the commits of transactions that stamped a catalog
  version, and a transaction opened with none under way and none started
  since names the catalog it sees by that count
  (`Transaction::catalog_epoch`); a listed database read under one count is
  not read again by a transaction under the same count, so a listing premise
  costs nothing per table until a catalog commit. On a store other
  processes write (TiKV, IndexedDB) no count names the catalog, and a
  transaction that links a unit kept under a listing — a stage set, a
  delete's edge cascade, the `REFERENCE` fields its repairs are lowered
  from — reads the listing once: two scans over the keys of the database's
  tables and its catalog versions, work in the number of its tables. No key
  a node reads in one access changes with every table's catalog without
  catalog writes to different tables conflicting over it.
- **Uncommitted DDL**: table versions are minted per write and visible only to
  their writer until commit, and functions are identified by content, so a
  unit compiled against uncommitted DDL is served to no other transaction
  until that state commits, and to none after a rollback. Planning over a
  block's speculative overlay neither looks up nor keeps units.
- **Not kept**: a body that binds a value from the planning context (a session
  value or client parameter read from outside, in it or any function it
  calls), a body reached from inside another body's compilation (the outer
  unit holds it), a `VERSION`ed call, a body the VM does not run.
- **Specialization**: a small body is still inlined into its caller rather than
  linked.
- **Catalog units**: what a plan lowers from the catalog alone, the same for
  every statement of a database that needs it, is kept the same way
  (`vm::units::CatalogUnit`): the read of a table a dispatch runs; a
  delete's edge cascade, a bundle whose entry is the edge delete; and the
  stage set of a write that may reach any table — the edge delete's among
  them — found by a digest of the bound statement and the runs of stages its
  body takes and compared whole (`StageSetUnit`). Each is keyed by what it
  is, the instruction-set version, the frame and principal, the depth, the
  names bound around it and the capabilities, and linked while
  the catalog it read holds and the planning context holds the same value
  for every name the definitions it read refer to, and, for a stage set,
  every name the statement reads inside what its lowering binds as a
  statement of its own, which binding may fold into its code. A unit linked
  under a listing it read earlier is noted as a read of the listing, so a
  unit that links it depends on the listing too.
- **Stage sets by table**: a stage set holds one member per table of its
  database and one for the tables the catalog does not hold, each the
  bundle of that table's stages — an arena of its own whose entry is the
  stage program — kept with the catalog state and context values its own
  lowering read (`vm::units::SetMember`). The set depends on the listing of
  the database's tables; after a change to any table the next plan lowers
  the set again from the listing, read once, taking every member whose own
  dependencies stand — the tables of the database judged by the catalog
  versions the listing holds, with no read per table
  (`CatalogDeps::hold_within`) — and lowering the members of the tables that
  changed and of those whose definitions read them (a person's stages repair
  the posts whose `REFERENCE` fields name a person, so a change to `post`
  lowers the member for `person` again). A member's dependency on the
  `REFERENCE` fields is the fields that can name a record of its own table
  (`CatalogReads::references`), found from those read before by reading the
  fields only of the tables whose catalog version changed
  (`Transaction::reference_fields`), so a change to a table that defines no
  such field leaves it standing. The stages of a member of an edge cascade's
  set make edge cascades of their own, for the same actor, that are the
  cascade running them: the member's arena does not hold it, and the
  instruction that runs it (`ReferenceInstr::PurgeEdges` with no unit) runs
  the cascade the context it runs in names (`Context::vm_edge_cascade`),
  which the edge cascade sets for the stages it runs. A write lowered inside
  a unit in flight (an event's clause, a cascade's statement, a function
  body), or by a planner that keeps no units, holds the stage units of every
  table in the arena being lowered, where its cascades and events close on
  the units in flight. Every table's stages are lowered when the statement
  arrives because whether the VM runs it is decided then: a table whose
  stages the VM does not lower fails the statement, because a write's
  stages, unlike a table's read, have no host form a run could take for one
  table. So the first plan after a schema change
  lowers the stages of the tables the change reached and reads the listing
  once; what remains linear in the tables of the database is the listing's
  two scans and a comparison of each member's catalog versions against it.
- **What a schema change still costs the next plan**: the stage set and the
  edge cascade depend on the listing, so after any change to a table of the
  database each is lowered again — the edge delete bound, mapped and
  lowered, and the set rebuilt from its members — at a cost independent of
  the tables of the database beyond the listing. A member that lowers a
  write that may reach any table inside it (an event's clause or a
  cascade's statement that writes `type::table(…)`), and every write
  lowered by a planner that keeps no units (a function body, a
  `VERSION`ed read, a speculation's overlay), holds the stages of every
  table and depends on the listing: such a member is lowered again after
  every change, at a cost linear in the tables of the database. The member
  for the tables the catalog does not hold repairs the records of any
  table, so it depends on every `REFERENCE` field of the database and is
  lowered again after a change to any table that defines one. A member's
  repairs run the stages of each table whose `REFERENCE` fields name its
  table, so a change to such a table lowers again the members of every
  table it references. On a store that names no catalog epoch (TiKV,
  IndexedDB) each transaction that links a unit depending on `REFERENCE`
  fields reads the listing once to find them.
- **Reads of tables only the data names**: a program holds the read units
  of the tables its sites name by analysis — record ids and tables written
  in the text, and the tables the kinds of the fields a `FETCH` walks name —
  and never lists the database for a read. A site that may name any table,
  and a record of a table its analysis did not name, is read through the
  unit of the table the data names, which the run links the first time it
  meets the table (`vm::dispatch::Deref`), in a counted fault
  (`vm::fault::link_read`): the node's unit where one stands for the
  table's catalog, and otherwise one the host lowers for the frame the
  program started in. A run links each table once per frame and catalog
  state, and every program of the run — a statement run once per record
  beneath a write included — finds the link (the run's table,
  `vm::closure::ClosureUnits`); a link stands only while the transaction's
  table catalog stamps are those it was linked under. A program keeps the
  links of the first tables its reads meet, so a record of a table met
  before is read with no lock or lookup. A statement whose records by id
  its caller knows the table of — a delete's repair reading the records a
  reference key names — is lowered with that table's unit
  (`vm::dispatch::seal_knowing`) and links nothing. A read that waives the
  frame's permission checks links only the unit it reads with. A table
  whose read the host lowers no unit of is read by the host's
  helpers, what they compile compiled in that fault. A table a value in
  `FROM` names is read under the statement's paging as a table named in
  the text is: the unit's run is told how many rows the pipeline takes
  before its `START` and `LIMIT` are spent (`execute_program_reading`), its
  read of storage asks for no more records than that first
  (`State::scan_hint`), a region over its rows holds no more than that in
  flight until it first hands rows on (`Rows::reading`), and the rows are
  handed on that many at a time, so a reader that has its rows stops the
  read; a reader whose predicate keeps fewer reads on in full batches.

A block is compiled in full when the query arrives: every statement it runs
is a group of its program, `EXPLAIN ANALYZE` of a statement included, and
running the block plans, binds and lowers nothing. A block holding a
statement the VM does not lower as a group fails the block whole. A block
whose catalog changes are not all modelled ahead of it — one holding a
statement that holds a schema statement inside it, or whose schema
statements change a table it reads by a value in `FROM` only the data names
or by a `FETCH` — plans the statements after the change late-bound: each
loads the tables, functions and parameters it names by the version the
transaction reads when it runs, its writes through their tables' write units
(`VM_BLOCKS.md`, *Statements planned late-bound*).

What still compiles while a program runs, and the plan for each:

| Site | Why | Plan |
|---|---|---|
| A write's stages for a table known only from row data (`vm::write`) | the table is named by the data | per-table units or a generic path |
| Cascade repairs and nested cascade statements (`vm::write_cascade`, `vm::subroutine`) | lowered when a delete first reaches a referencing table | lowered with the referencing table's catalog when the delete is planned |
| A record read through a table only the value names (`execute_record_lookup`), which compiles the table's permission | the table is named by the data | the same per-table units |
| A permission predicate reading a call's own parameters, and the `CompiledGuard` memo | the predicate is compiled for the call | compile with the plan for the predicate's frame |
| A call a planned body does not stand for: a `VERSION`ed call, a redefinition the plan did not see | the definition differs from the one planned | plan invalidation |

## Where the VM is not there yet

| Area | Today | Target |
|---|---|---|
| Physical plan | `Plan::Write` carries the `LogicalWrite`; `Plan::Ddl` carries the statement. Blocks are control nodes over physical plans (`Plan::Control { control }`), the source statements held beside the plan for the footprints the lowering reads | write effects and DDL effects as abstract nodes |
| Blocks, `FOR`, `IF`, `LET` | control nodes `Seq`, `Let`, `For`, `If`, `Throw`, `Return`, `Break`, `Continue`, `Sleep` in the physical plan (`vm/plan.rs`), lowered into one program: loops and `IF` branches are jumps, signals are jumps, and each statement — schema statements and `INFO` included — is a group of the program's own pipelines (`Program::groups`), sharing its tables, run by `RunEmbedded` over the program's one `State` — its paging, sorts, aggregates, permission frames and write state started afresh per reach — with its tail (`TIMEOUT` a deadline the host enforces, `VERSION` its sources' stamp, `ONLY` its cardinality check) compiled with it. The block's own expressions (a `FOR` range, an `IF` condition, a `RETURN` or `THROW` value) are evaluated in the control pipeline, or, holding a statement, are expression statements whose statements are groups of theirs. `LET` and `FOR` names are slots of the state read by `LoadSlot`, bound in the frame too only where something the program does not lower may read them; a `LET` of a literal is a constant the statements in its scope read as written, and an `IF` condition it settles decides its branch when the block is mapped. A block's schema statements whose effect is known are run speculatively while it is planned, and the statements after them planned against the overlay they leave (`vm::speculate`): a schema statement known to do nothing is folded to `NONE`, the statements after one known to raise are not compiled, a loop defining only on its first iteration is peeled, and a write lowers no event or index stage its table has nothing for. A block whose catalog changes cannot all be modelled — a schema statement in a loop or under a data condition, one reached through code a statement runs, a peeled loop that may run no iteration — and that runs a statement reading what they may change plans what follows the change late-bound, loading each catalog object it names when it runs (`fault::serve_stage_bundle`, `vm::call_unit`, `vm::param_unit`); a statement after the block's first schema statement that the planner refuses raises the refusal when the block reaches it. Every statement is lowered once, when the query arrives, so a loop's body is lowered once for all its iterations, and nothing is compiled while the block runs. A write's catalog reads (its tables' definitions, its gate's answers, its stage programs) are kept for the run across reaches. A statement nested in a loop body's statement, kept once per run of that statement, is kept across the loop's iterations when it reads no name the loop binds and nothing in the body may write (`NestedSpec::hoist`). The context-change guard hands the VM a statement whose schema statements are all statements of its control flow. `EXPLAIN ANALYZE` of a statement is the statement's group, run by `BlockInstr::RunAnalyzed`, which renders the plan text fixed when the query arrives over what the run measured (`Program::analyses`); a semi or anti join the logical rules lifted out of a filter is mapped as the membership it was lifted from. A statement the VM does not lower as a group fails the whole statement holding the block. Still: a write's instructions read its names from the frame, so a write loop binds them there each iteration, and a reach compiles the write's clauses and builds its permission frames afresh (see `VM_BLOCKS.md`) | names read by writes from slots |
| Subqueries | a statement in scalar position the plan maps with the statement (`PhysicalPlan::nested`) is a group of the enclosing program, run by `BlockInstr::Nested` over the program's one `State` in the frame it is evaluated in, the row bound as its `$parent` and `$this`, a `RETURN` it signals caught at the root of the expression holding it. Its strategy is the optimizer's, from the facts mapping records (`NestedFacts`): **once** — run by the first row that reaches it, its value kept until the group it is evaluated in starts again — for a read that reads nothing of the row, whose scalars (permissions and computed fields included) are repeatable, and whose statement writes nothing; **hash** — a `Plan::Probe` beneath the join predicate, its build (the table's read, permission and layers) run once by the first evaluation that opens it and its rows kept, keyed by the join field, until the group it is evaluated in starts again, each evaluation running the rest of the statement over the kept rows its key may equal — for a read whose only reading of the row is the first conjunct of its innermost predicate, `field = $parent…`, which is otherwise repeatable, and whose statement writes nothing (`JoinKey`); **per-row apply** otherwise, and always for a statement that writes. The probe yields a superset of the rows the predicate admits, which the predicate still judges, so an error is raised for the rows the per-row run raises it for; numbers are indexed by representation, as their hash does not agree with `=`. A statement in `FROM` is a group of the program too (`Source::Nested`). Still: an applied statement reads its correlated values through its frame, not registers; the index nested loop is not chosen, since no index access takes a key evaluated per row, and an applied read over an indexed key reads its table whole per row; a probe holds its build's rows in memory, not leased from a pool; a join on several keys, on a key read through anything but a path into `$parent`, or on a conjunct other than the first stays applied; a pipeline holding a probed statement is no region; a semi or anti join the logical rules decorrelated into is put back as the membership predicate it was lifted from (`vm::map::memberships_of`); a pipeline holding a nested statement is no region, so an applied read over many rows runs serially; a statement in a host pipeline (a paging clause, a write's data clause or `RETURN`, a graph hop's `WHERE`), in an aggregate's argument, a nested block or schema statement, and one the plan does not map run as units of the enclosing program (`SubquerySpec`), each a program of its own over the context a subquery derives, and a subquery the VM does not cover fails the statement holding it | pipelines or subroutines of the enclosing program, the index nested loop, and multi-key joins |
| Expressions | instructions: fields, over rows that are objects and (a host `Access` step) rows that are not; `$this`, which is the element inside an element body; `@`, the record identity of the current value; `.*` (`Each`, a host step when it fetches a record); `[WHERE …]`, a loop over the array's or set's elements with the element as the current value and `$parent` bound when the predicate reads it; a path's broadcast over an array (`MapBegin`), the element the current value of the rest of the path; an optional chain (`.?`), a nullish jump with the rest of the path over the base; a destructure (`Destructure`, a host step fetching a record id, its aliased bodies host pipelines over the value destructured); mocks (`Mock`, under the allocation limit); a recursion (`.{..}`, `+collect`, `+path`, `+shortest` to a constant), a `Recurse` host step taking the host's recursion strategies with its body a program of its own over each depth's value; `THROW`, and `BREAK`/`CONTINUE` as signals (`Signal`), a record id key element taking one for its own error. A computed column over the reduced record is instructions reading the reduced record as its element. Graph and reference lookups (`->`, `<-`, `<->`, `<~`, with a `WHERE`, `ONLY` or a `*` projection) are one `Lookup` host step over the adjacency and reference keys (`vm::graph`), a `->edge->vertex` pair the planner reads from the adjacency key one hop, with each hop's table permissions, field views and `WHERE` host pipelines of the program lowered from the catalog at lowering (a `WHERE` the planner judges from the key alone judged from the key); in a computed field's frame of the caller's own principal, a permission predicate's or a write gate's — whose reads are unchecked — with no `WHERE` and no projection; a hop table whose permission reads `$parent` judged with `$parent` the row the lookup is taken over; a wildcard hop's records, and every hop's under a `VERSION`, resolved as the scans resolve them. A hop with clauses — a projection, `SPLIT`, `GROUP`, `ORDER`, `LIMIT`, `START` — is the statement they make over the records the hop reaches shown whole, `$parent` the row, a group of the program run per row; a last hop whose only clauses are an integer `LIMIT` and `START` is one `Lookup` step that reads the first `start + limit` entries of each origin's adjacency and takes the page of what the origins reach. A closure literal is `MakeClosure`, whose value is a `CompiledClosure`: a code reference — the SHA-256 digest of the closure as written, its parameters with their kinds, its return kind and its body (`ClosureDigest`) — and its captures, with the closure as written for a host to compile from and no compiled body; the body is lowered with the program as a unit of its root (`ClosureHead`) — for every literal the program makes and every closure a parameter holds at plan time, for the planner's principal, database and enforcement — and the unit a value runs is linked by the instruction that hands the value to a callee — `Invoke`, a builtin call at the positions the builtin invokes a closure from (`ScalarFunction::closure_args`), a method — which yields `Step::NeedUnit` first for a closure its state has not linked in the instruction's frame (`vm::closure`); the host resolves the digest in the run's table: the units the running programs lowered, then the units the run linked, then the node's units (`vm::units`, keyed by the digest, the instruction-set version, the frame and the principal, linked while the catalog the lowering read holds), each taken in a frame it admits, for a value whose closure is the unit's (compared whole where the two are not one, since closures that print alike share a digest), and while what its lowering read holds; a unit's head declares what its body may do beyond computing its value (`ClosureHead::effects`: writes, schema statements as writes, reads of rows, the world outside, bodies it cannot see), and the instruction linking it refuses the invocation where the frame does not grant those effects (`vm::closure::grants`): a speculative run grants only what its real run answers alike and what acts on nothing outside the transaction, while a frame that refuses writes, a read-only transaction and a missing capability refuse an effect where the body reaches it, with the error the effect raises, as a body that does not reach it runs to its value; a miss in all three is a fault, counted, in which the host compiles the body through the physical planner for the invoking frame (`vm::fault`, reached only from that step and from an invocation inside the instruction's call whose link stopped standing), the node keeps the unit and the run links it, so a distinct closure faults once per node and catalog state; the invocation — a call or a closure-taking builtin — then only finds the linked unit and runs it, in place in the caller's frame, its arguments and captures in slots, when it reads nothing else of the frame; a unit is lowered for the `VERSION` stamp the invocation reads at and runs only invocations at it, an invocation from a computed field mid-computation runs it with the record being computed, one with a current value and no document runs it over the value as no document (`State::undocumented`), and one outside a running program loads the unit for its frame from the compile cache and runs it in a table of its own; an invocation inside a builtin's call whose linked unit stopped standing during the call (a body that changed the catalog, a table past the run's bound) cannot hand the step back, and takes the instruction's fault itself (`vm::closure::relink`), counted, linking the unit again; a body the host lowers no unit of holds the failure — the error compiling it reports, or the reason the VM does not run it — which each invocation reports, compiling nothing again; so a running program compiles only in faults (`compiles::count` is zero), and the scan of the modules a running program reaches the host through admits, of the closure value, only the members that compile nothing outside a fault; a closure value is never stored (a write storing one fails), so no reader finds one in stored data; an `Invoke` that finds its unit no longer standing yields for it again; `$f(args)` is `RequireClosure` and `Invoke`; a method no builtin names is `ClosureField` and `Invoke`; a closure written in `array::map`/`filter`/`find`/`any`/`all`/… or the array method of the same name runs in place when its body reads only its parameters, the row, the caller's parameters and builtins; `type::field`/`type::fields` called for their value are calls of the program. An expression with no instructions fails its statement, a projection-layer column or guard included: one holding a statement that reads the table it is a column of, whose source the bind leaves for its scan to enforce, fails the statement | expression bytecode; graph lookups as sources of the program; recursion bodies as pipelines of the program invoking them, and the recursion strategies as instructions; closure bodies lowered with the query rather than where they are first invoked; a closure value the engine did not compile — a `Closure::Expr` a `DEFINE PARAM` holds, and an API continuation — which no unit stands for, so a call, a method and a closure-taking builtin hand it to the continuation's stage (`invoke_uncompiled_closure`); and a content digest over a structural encoding of the closure rather than its canonical text: the logical form carries the statements a body writes whole, as the expression layer's statements, whose one canonical encoding is their SurrealQL |
| Sources | VM sources: a value, array, mock, parameter or expression in `FROM` is `Source::Values`, evaluated when its pipeline opens and read as `ValuesRead` states — a record id among the values read under the caller's permissions and computed fields, a dynamic read's record id under the zero-limit contract, and a table a parameter or call finds read by `SELECT *` over it, bound and lowered when the run first finds it and kept for the run while the catalog holds (`vm::nested`); a statement in `FROM` is a group of the program, its own reads of storage bound to its reader's zero limit unless it writes, and a row of it that is a record id read as the record by `SELECT *` (`Instr::FetchRow`); a union mixes them with scans. Still: record ids are read by the host's fetch helpers (`fetch_record`, `execute_record_lookup`) as host effects; a table found at run time is a program beside the statement's; `FOR UPDATE`, `WITH` or a resolved schema over a value source, a block or schema statement in `FROM`, and a sort key path over rows that may not be objects are not covered | record reads as instructions; a table found at run time a specialisation lowered into the program |
| Write storage stages | instructions: the record, edge-side and pointer adjacency keys, inline pointer payloads, reference keys, a delete's record, cache and adjacency keys, B-tree and `UNIQUE` index entries, and doc-id release, each key encoded by the VM's codecs. Every index of a table is one instruction, in the catalog's order, over host pipelines of the program lowered from the definitions the catalog held at lowering (`LoweredIndexes`, one `IndexPlan` per index): the columns, and for a `COUNT` index its guard. A B-tree or `UNIQUE` index's entries are written by the host; a full-text, HNSW, DiskANN or `COUNT` index is moved through its kind's own implementation from the values the pipelines answer. A record whose table holds other definitions when it is written fails the statement. Host effects: an in-flight index build taking a change (`IndexBuilder::consume`), a doc-id's resolution and release (`TableDocIds`), and the settled read of a table's graph marker. A delete's cascades are instructions (`vm::write::cascade`): the edge cascade (`PurgeEdges`, a peek at the adjacency and the edge delete), the drain of the reference keys naming the record, a loop taking each key's referencing record in turn (`Next`), its repair (`Repair`) and the keys' removal. The repairs are data on `Repair`: every table with a `REFERENCE` field that can name a record of the table written (any table, for a unit standing for a table the catalog does not hold), and for each field its strategy, lowered when the statement arrives: nothing for `IGNORE`, a refusal for `REJECT`, a unit for `CASCADE`, three units for `UNSET` (the read of the referencing record, the update that unsets a link, the update that subtracts the deleted record from a collection), and for `THEN` the definition's expression as a host pipeline of the stage program, evaluated in the purge's frame (permissions off, `$reference` the deleted record, one level deeper) with the referencing record's id as the current value; a failed repair names the deleted record. Each statement a cascade runs is a unit of the arena of the statement being lowered, found again by what it is and the frame it is lowered for, so a cascade that reaches itself (a ring of references, a chain of the same table) closes on the unit being lowered; the edge cascade is a bundle of its own arena, its entry the statement, which the node keeps, and an edge whose delete cascades to edges runs again the edge cascade the context names. A run binds the record the statement is about and runs the unit one level deeper, in the frame a cascaded delete runs in (a cascaded delete unenforced for the referencing record and enforced again beneath it; an `UNSET` repair unenforced); it plans, binds and lowers nothing, and the levels run as frames of the driver rather than on the thread's stack. A write statement of a cascade runs over a state kept from one run to the next while the transaction's catalog is unchanged, so its write state keeps the stage programs and catalog reads of earlier records; a run reached while another of the same statement is in flight, around a cycle, takes a state of its own. A key naming a referencing table the repairs do not hold is an internal error; one naming a field of such a table that it does not hold fails as the catalog's lookup of the field does. | the read of a referencing record as a host read with a derive pipeline |
| Reads by data after a block's schema statements | the program's table dispatch holds units lowered against the catalog the query arrived with for the tables its sites name, and links the units of other tables against the catalog the run meets. A block whose schema statements define, alter or remove a table or a field that a `FROM` value only the data names or a `FETCH` may read fails the block; a step through a record id (`$r.c`, `$r.*`) after such a schema statement reads through those units where another read of the program built one for the table, and misses what the schema statement changed | units lowered against the catalog each statement meets |
| Write structural checks | done: record exists, not a view, not lightweight, table type are instructions | — |
| Write observers | events are instructions (`vm::write_observe`): a table's events for an action are an observer unit of the statement's arena (`ObserveInstr::Events`, `Program::events`) whose host pipelines are each event's `WHEN` and `THEN`, lowered once from the table's definitions and dispatched in the event frame, with `$input` materialised from the writing program's data clause before the unit runs. A clause the VM answers natively is the pipeline's instructions; a clause that is a statement is a program of the VM's own, bound and lowered with the event's planner (the writer's catalog, permissions suppressed, `$before`/`$after`/`$event`/`$value`/`$input` left to each record) as a unit of the same arena, and run as a subquery whose `RETURN` is the clause's value. An observer unit is found again by what it runs and what it is lowered with, so events that write back to their own table, directly or through another table's events or views, close on the unit being lowered and run by id; a chain of event frames that never settles is refused once four lowerings of one table's events for one action are open. `ASYNC` firings are queued as a host effect; a record whose table holds other events than the unit was lowered from fails the statement. The change feed entry is a host effect (`RecordChange`). Views are instructions (`vm::write::view`), lowered with the write's program from the table's view definitions: each view's condition, projection, group key, aggregate arguments and fields are host pipelines run in the view frame (permissions off, writes allowed, the writer's authority), and the row a view computes is written through the VM's record codec and taken through the view table's own order (its indexes, views, events, live queries and change feed) as a write of the view-row form (`WriteForm::View`, whose subject is the row and whose action the maintenance settled): an observer unit of the statement's arena lowered from the view table's definitions, one pipeline per action, run over a write state stacked on the writer's in the view table's view-maintenance context. A view of a view lowers its view table's order in turn; a cycle of views closes on the unit being lowered; a record whose table holds other views than were lowered fails the statement. Live subscriptions are units of bytecode (`vm::live`, `ObserveInstr::Lives`): each subscription's table permission, field view, `WHERE` and projection compiled for its principal when it registers, or when a write to its table is planned against a catalog version the node holds no unit for, keyed by its id and the table's catalog version, and looked up by the write, which compiles nothing while it runs; `FETCH` is the host's, and a subscription with no unit is notified by the host's own computation (see `VM_WRITE_STAGES.md` §6). Recomputing a group's extremes is a VM subroutine, lowered once per statement and set of extremes, the group's key values bound as parameters (`NestedKey::ViewRecalculation`); an aggregated row's stats (`Metadata::aggregation_stats`, every `AggregationStat`) are in the VM's record codec, byte for byte the Rust codec's | an instruction for every expression a clause holds (see the expression rows) |
| Evaluating stages' edge cases | an id `DEFAULT` with no logical form; statements or user-defined calls in field-permission predicates under a narrowing `AUTH LIMIT` | lowered |
| DDL | a schema statement the VM covers is instructions over the catalog's keys, calling no statement body: the checks it makes of a definition against the catalog (`DEFINE FIELD`'s of the table's fields and indexes) are made when it is lowered, against the catalog it runs against, and a refusal is an instruction raising it where the body raises it (`StoreInstr::RefuseField`); an index's build is the index builder's host effect. A schema statement the VM does not lower fails the statement | lowered |
| Sorts and group maps | a sort under `TEMPFILES` spills sorted runs past `external_sorting_buffer_limit` and merges them (`vm::spill`); every other sort and every group map is held in memory, unbounded | a per-query memory budget, spilling past it |
