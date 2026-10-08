# RFC: Precompiled queries and compiled catalog units

**Status:** Draft · **Scope:** `dbs/executor.rs`, `exec/vm/` (map, lower, units, dispatch,
deferred, fault, closure), `exec/logical/`, `datastore/src/tx.rs` (catalog versions), the catalog
key space · **Implements:** `EDGE_PLAN.md` rules 1–3, triage items T7 and T8

## 1. Problem

A query is compiled when it arrives, and a running program is never recompiled. The running
program reads KV, compares, does arithmetic, builds values and calls builtins. It never plans,
lowers or reads the catalog (`EDGE_PLAN.md`, "What the VM does"). Code it needs but does not hold
(a catalog object's unit, or query text that exists only at run time) arrives as a block from the
compile service (§3, §9).

Compiling only when a query arrives runs into a basic fact: **the catalog a statement runs
against is not always the catalog the query was compiled against.** That happens in three ways.

1. **The query's own DDL.** A statement after `DEFINE FIELD`, `DEFINE INDEX`, `DEFINE FUNCTION`
   or `REMOVE TABLE` runs against a catalog the DDL changed. Where the DDL is unconditional, its
   effect can be modelled at compile time. Where it is conditional (`IF`, `FOR`, a function body,
   a value only known at run time), the compiler cannot know which catalog follows.
2. **Outcomes.** A `BEGIN`…`COMMIT` group that fails or `CANCEL`s rolls back its DDL, so the
   statements after it see the catalog as it was before.
3. **Other clients.** A statement outside `BEGIN`…`COMMIT` runs in a transaction of its own.
   Each later transaction reads a newer snapshot, and another client's DDL may have committed in
   between. No compile-time model of the query predicts that.

Separately, **some targets are named only by data**:
- a `FETCH`, or a dereference of a record link to a table the query never names;
- `FROM $x`, `type::table($t)`, `type::record(...)`;
- graph hops to edge tables chosen at run time;
- the table of a record a reference cascade reaches.

The compiler cannot specialise for a table it cannot name.

The engine today resolves these by compiling at run time, in four places:
- the executor plans each top-level statement as it reaches it (`dbs/executor.rs`
  `plan_statement_inner`);
- a block plans a statement after unpredictable DDL (`Control::Deferred`, `vm/deferred.rs`);
- a subquery planned without a transaction is planned when it is first evaluated
  (`DeferredSubquery`);
- a closure or a table the plan did not name faults to the host, which lowers a unit
  (`NeedUnit`, `vm/fault.rs`).

`enforce_row` and `fetch_record` also compile field-state and permission expressions with a
planner that has no transaction (`host.rs` → `operators/scan/pipeline.rs`).

This RFC replaces all of them. Query text that exists only at run time is never a recompile of
the running program: `eval::surql`, `eval::gql` and embedded JavaScript's `surrealdb.query()`
run as nested queries the executor compiles as programs of their own (§9).

## 2. Design summary

- **Every catalog object that holds code runs as bytecode.**
  - Tables (reads, and writes with their index maintainers, events, views and changefeeds),
    functions, params and access methods are compiled into units (§3). Index maintenance, event
    firing and view derivation are one compiler's output, so a fix to it reaches all of them.
  - The catalog stores definitions only. A unit is derived from (object, version, build) and kept
    in a shared compile cache in KV. It is disposable: a new build misses and recompiles, so every
    unit picks up every fix in the build that runs it, with no migration.
  - Units bind late: a unit calls another object through that object's unit, so it depends only
    on its own definition.
  - Within one build, units have a fixed ABI: the calling convention, the descriptor formats, and
    a unit header carrying the build identity, the object's version and the unit's effect
    summary.
- **Inlining is a logical-plan concern (§4).** The logical plan inlines:
  - the query's own closures, `LET` values and constants;
  - the standard library;
  - catalog definitions (function bodies, param values, permissions), read once when the query
    arrives.

  Under snapshot isolation nothing outside a transaction changes what it reads, and a query's
  catalog is pinned at arrival (§4.2), so inlined logic never changes while the query runs. The
  one exception is the query's own DDL. The logical plan sees it in the query text, including DDL
  inside a body it inlines, and calls the unit of any object that DDL may touch instead of
  inlining it.
- **Units inline the same way, inside themselves.** A unit is compiled from one definition and
  the standard library. It calls every other catalog object through that object's unit.
- **The query calls units where it does not inline**, for example after its own DDL on an object,
  or for a target only data names.
- **Catalog facts are hints to the physical plan, never premises (§6).** The physical plan may
  shape a pipeline from what it saw, for example an index-probe pipeline beside a scan. What the
  pipeline does is decided by the descriptors it loads when it runs (§5). A stale hint costs speed,
  never correctness, so the program holds no catalog assumption and nothing is checked or
  invalidated.
- **Targets named only by data** always take the unit path (§7).
- **Execution is bytecode blocks over morsels (§11).** Compiling is a service that produces
  blocks; running a block does not depend on where it runs.
- **Run-time query text:** `eval::surql`, `eval::gql` and `surrealdb.query()` from embedded
  JavaScript run as nested queries through one named seam, `nested_query` (§9).

There is one compiler and one VM. A running program is never recompiled; new code only arrives as
new blocks.

## 3. Compiled catalog units

### 3.1 What is a unit

A unit is an encoded `Program`, or a small set of them, compiled from one catalog object's
definition at one version by one build. The generic routines a unit is built from (index
maintenance, write stages, sequences, event and changefeed dispatch) are a standard library
written in SurrealQL with an internal intrinsics namespace. Its logical plans are produced at
build time. The physical layer plans them for each object's definition, so a table's write unit
is the write routine planned for that table's indexes. Only SurrealQL is a stable format; logical
plans, physical plans and bytecode are derived and cached (`EDGE_PLAN.md`, "Target design"). The catalog stores definitions only; units are derived
state, held in a compile cache (§3.4). The objects that get units:

| Object | Unit contents | Why |
|---|---|---|
| Table | **Read unit**: decode, the `SELECT` permission, computed fields, field-level permissions. **Write unit**: the schema gate (field kinds, `ASSERT`, `VALUE`, `DEFAULT`, `READONLY`), the per-operation permissions, index maintenance (including `UNIQUE` checks), events, table-view derivations, reference bookkeeping, changefeed emission | Every read and write of the table, including those of a table only data names |
| Function | Body, the `PERMISSIONS` predicate, the `AUTH LIMIT` cap | Every call, from queries and from other units |
| Param | Value and `PERMISSIONS` predicate | Reads under a permission |
| Access method | Signup, signin and authenticate bodies | These evaluate SurrealQL today |
| Event | Condition and body | Part of a table's write unit, kept separately addressable so the write unit calls it |
| Model / module | Entry stubs | The calls are external; the stub is the guard and the argument coercion |

**Index maintainers, events and views are bytecode.** Which indexes a write touches, the keys it
builds, `UNIQUE` checks, the order of the work, event conditions and bodies, and view deltas are all
lowered by the one compiler. Algorithmic kernels stay native and are called from the bytecode as
host instructions, the way builtins are: HNSW graph updates, full-text tokenising and postings,
geometry. B-tree and `UNIQUE` maintenance already run as VM instructions at parity with the
operator tree (`VM_REGRESSIONS.md` R7: indexed insert 38.65 ms against 39.11, unique insert 38.57
against 39.31, release, 1000 rows).

A table's unit is compiled from the table and everything defined under it: fields, indexes,
events, views over it. Its version (§3.7) changes when any of those does.

### 3.2 Late binding

Inside a unit, a reference to another catalog object is a call through that object's unit, never
an inlined copy. For example:
- an event body calling `fn::notify` loads `fn::notify`'s unit when it runs;
- a computed field reading `$config` loads the param's unit;
- a view over `person` loads its target table's write unit.

That keeps the dependency of a unit on exactly one definition, so a DDL recompiles exactly the
units of the object it writes, and never fans out to dependants.

A query never inlines a unit's code; it calls the unit (§4).

### 3.3 Principal-generic

Units are keyed today by principal and frame (`vm/units.rs` `UnitKey`: `auth`, `UnitFrame`,
`unchecked`, `depth`, `shadowed`, `capabilities`). A shared unit cannot be: it is compiled once
per (object, version, build) and served to every caller. So in a unit:

- **Permissions are instructions.** The unit branches on the session's principal class (root,
  namespace, database, record user of an access method), and evaluates the predicate where one
  applies. "Permissions bypassed" is a run-time flag the unit reads, not a compile-time fold.
- **`AUTH LIMIT`** is an instruction that caps the principal for the call's duration.
- **Capabilities are checked when the call runs.** A unit that calls a builtin a capability gates
  checks it the way `Step::CallAllowed` does today.
- **Recursion depth** is a run-time counter.
- **`unchecked`** (evaluated where reads are not permission-checked) is a run-time flag.
- **`shadowed`** (names bound around the call that the body must not resolve) cannot reach a
  unit. A unit's body is bound only to its own parameters, and the caller's names are not in
  scope.

The node's in-memory unit cache becomes the first tier of the compile cache (§3.4).

**The node tier (`vm/compile_cache.rs`).** Function units and table read units are kept under
(ns, db, object kind and name, object version, build identity). The version is the object's
catalog version, or the content hash of a definition no write of this build has stamped. The build
identity is the ISA, the release and the node's capabilities: planning folds capability checks into
constants, so capabilities stay in the identity, not in a per-principal key. Under one key there is
one compilation per frame it is linked into:

- The frame names the class of the principal (`vm/program.rs` `PrincipalClass`), never its
  identity. Planning consults its principal only through whether its reads of the database are
  checked, for itself and for itself capped by each `AUTH LIMIT` it compiles under. Each of these
  decides every such answer under every chain of caps: a record or anonymous principal's checked
  bit; a system user's level kind, whether that level covers the database, and whether it holds a
  role. Every principal of a class therefore compiles the same code. A unit's frames admit that
  class alone, and a run under another class fails closed.
- The frame also names the permission contract: suppressed permissions, and checked reads.

Closure bodies (a unit, or the failure a body no unit of which is lowered reports), a
table's field state and `SELECT` permission, and the stages a write that may reach any table
applies to one table are kept the same way. A closure is keyed by the closure as written, compared
whole, with `ObjectVersion::Inline`. A write's stages are keyed by the bound write, the table and
the table's catalog version. A running program loads each by name and compiles only on a miss.

A unit is compiled at the depth of a statement, binding only its own arguments. It reads the
session's own values (`$auth`, `$access`, `$token`, `$session`) when it runs, so a permission that
reads `$auth` serves every record user. A miss is single flight. A compilation that requests another
unit while it compiles never waits on a flight it does not lead. A unit is kept with what else its
compilation read: the bodies its bundle holds, the tables its plans read, and client values its
binding folded in. Those are checked when the unit is served, and each frame keeps up to four units,
one per state of them.

A statement planned late-bound (`VM_BLOCKS.md`, *Statements planned late-bound*) loads three
further kinds from the same tier, by the version its transaction reads when it runs:

- **Table write units**: a late-bound write names its stages as a write that may reach any table
  does (`StageTables::by_name`), and its run loads the bundle of a table's stages by the table's
  name (`vm/fault.rs`, `serve_stage_bundle`), keyed by the write statement, the table and its
  catalog version, for the version each record's write meets. A bundle holds the statement's own
  stages beside the table's — its data clause, its output — because those read the table's view:
  the snapshot the data clause reads, the record-id default, the projection it returns. A
  table-generic unit the statement calls between its own stages, with those views read through the
  table's read unit, is open.
- **Call units** of user-defined functions (`vm/call_unit.rs`), keyed by the function's version
  and the call's arity: the call compiled for the definition, which makes the call's checks, binds
  and coerces the arguments and runs the body. A body that reads an argument where only a value
  answers (a `MATCHES` operand, an access path) is compiled for the values a call's arguments hold
  and kept by them, at most `specialised_body_cache_size` such bodies per node, least recently used
  evicted first. A body compiled for values whose encoding is larger than
  `specialised_body_max_arg_bytes` (default 4096) is compiled for its call alone and not kept.
  Values are keyed type-exact: `1`, `1.0f` and `1dec`, or `0.0f` and `-0.0f`, key different
  bodies. Once a function's body has been compiled for `specialised_body_values` distinct
  values, its calls run the body compiled to read the arguments when it runs, where that answers
  alike (no full-text or nearest-neighbour operator, no index function) and compiles.
- **Parameter units** (`vm/param_unit.rs`): a parameter's `PERMISSIONS` predicate, keyed by the
  hash of the clause, which judges a read whose definition holds a clause other than the one the
  program lowered. A parameter's value is read when it runs.

Security (`SECURITY_GUIDE.md`): no unit crosses a namespace or database, since both are in the key
by name and by id. A unit compiled against uncommitted DDL is keyed by a version only that
transaction sees. Sharing within a class is sound because the code is identical across the class. A
record user and an owner are never in one class. `AUTH LIMIT` capping, function `PERMISSIONS`,
capability checks on entry and the computation-depth budget are applied by the run, against the
run's principal.

### 3.4 The compile cache

- **Key:** (ns, db, object, object version, build identity). The build identity names the
  compiler and the ISA together, so two builds never share an entry.
- **Tiers:**
  - the node's memory, which is today's unit cache keyed by this key;
  - a shared cache in KV, under a key space of its own, outside the catalog.
- **A cache entry is never the source of truth.** It may be dropped at any time and is rebuilt
  from the definition. It is written outside the transaction that reads it, as a cache fill: an
  entry for (object, version) is valid for any transaction whose snapshot holds that version, and
  an entry written by a transaction that later aborts is harmless.
- **Old entries are collected** by version and build: an entry whose object version is no longer
  current, or whose build no node runs, is garbage.
- **Fans** read the shared tier. A cold miss asks the compile service (§3.5).

### 3.5 Who compiles units

The compile service compiles a unit on a cache miss, from the definition at the version the
requesting transaction's snapshot holds. A running program that needs a unit it does not hold
requests it from the compile service. This is the one kind of compile
after a query arrives, and it never changes the running program: it produces a block the program
calls.

When the query arrives, the compiler also requests the units of every object the query names, so a
miss is paid before the program starts wherever the object is known up front. The service may
also warm the cache after a DDL commits, compiling the units the DDL invalidated. That is an
optimisation, never a requirement.

A DDL therefore compiles nothing itself, and a schema import that defines a table and 500 fields
compiles the table's units once, when they are first used, not once per field.

### 3.6 Builds, upgrades, mixed-version clusters

- A unit is keyed by build, so a node only ever runs units its own build compiled. There is no
  cross-build ABI to keep stable, no decode of older encodings, and no catalog migration on
  upgrade.
- After an upgrade, the new build's cache is cold. It fills on demand, or by a warm-up pass over
  the catalog before the node takes traffic.
- In a rolling upgrade, each build reads and fills its own entries. They coexist in the shared
  tier.
- Units are not a persisted format of the catalog, so they owe no upgrade-test pair. The catalog's
  definitions still do, as today.

### 3.7 Catalog versions for every object

Per-table catalog versions exist today (`CatalogVersionKey`, `tx.rs` `catalog_version` and
`bump_catalog_version`). They are bumped by writes to a table, its fields and its indexes. The
in-memory unit and descriptor cache is keyed by version, so every object kind needs a version:
- events and views bump their table's version;
- functions, params, access methods, models and modules get versions of their own, minted the
  same way: a fresh uuid per write, never an increment.

`def_hash` checks in `vm/function.rs` and the stamp checks in `closure.rs`, `dispatch.rs`,
`subroutine.rs` and `write_cascade.rs` go away: nothing holds a stale copy to check.

## 4. Compiling the query

The query is compiled once, when it arrives.

- **The logical plan inlines:**
  - the query's own closures, `LET` values and constants;
  - the standard library's routines;
  - catalog definitions (function bodies, param values, permission predicates, computed fields),
    read once from the catalog pinned for the query (§4.2).
- **It does not inline an object the query's own DDL may touch.** DDL is visible in the query
  text, including DDL inside a body being inlined. The one exception is a script's
  `surrealdb.query()`, so a call that runs a script is treated as a call that may write the
  catalog (§9). So the logical plan knows every object the query may redefine. Those objects are called through
  their units, which load the definition the transaction sees when the call runs.
- **Layouts and access paths stay data at run time:** fast instructions over descriptors, with
  branches to the slow path (§5). The physical plan uses catalog facts only as hints to shape
  pipelines (§6).

### 4.2 The catalog a query sees

A query that spans several auto-commit transactions runs each in its own snapshot. Its catalog is
pinned when it arrives: every statement sees the catalog as of arrival, plus the query's own DDL
in order. Another client's DDL that commits between two of the query's transactions is not seen by
the rest of the query.

A write remains safe under a concurrent catalog change: a transaction that writes a table reads
that table's version key, so a concurrent DDL on the table conflicts with it at commit.

### 4.1 A function call is a unit call

A call to a user-defined function is one instruction: call the function's unit by (ns, db,
name). When the call runs, it loads the unit its transaction sees, through the
transaction cache keyed by the function's version. In this query:

```surql
DEFINE FUNCTION fn::foo($a, $b) { ... };
fn::foo(1, 3);
DEFINE FUNCTION OVERWRITE fn::foo($a, $b) { ... };
fn::foo(1, 3);
```

- the first call runs the body the first `DEFINE` wrote;
- the `OVERWRITE` compiles the new body and writes its unit in its own transaction;
- the second call loads the new unit, either by reading its own transaction's writes or from the
  committed state in a later transaction.

If the `OVERWRITE` fails or rolls back, the second call loads the old unit. No compile is
involved.

Both calls are unit calls, because the query's own DDL touches `fn::foo`. A query that never
defines `fn::foo` inlines its body in every call. The body is read from the catalog pinned for the
query (§4.2), and nothing can change it while the query runs.

## 5. Descriptors on the stack, and branches

The program is correct under any catalog, and fast where it can be: **the catalog's facts are
loaded onto the stack as data when the program runs, and fast instructions take them as
operands.**

- **Descriptor loads.** When a pipeline starts, one instruction loads an object's descriptor
  into registers through the transaction cache. A table's descriptor holds:
  - its record layout: field slots and offsets, the null bitmap, typed or untyped;
  - its index list, with each index's fields and kind;
  - handles to its read and write units;
  - whether permissions apply to the session's principal.
- **Fast instructions are parameterised by a descriptor register:** `ReadField dst, row, layout,
  field`, `IndexProbe src, indexes, conjunct`, `CallUnit dst, handle, args`. The same bytecode
  follows whatever descriptor the transaction loads.
- **A branch takes the less efficient path when the descriptor does not fit the fast form.**
  Examples: an untyped layout, a field with no fixed slot, no index matching the conjunct, or a
  permission that needs evaluating. The branch goes to the generic record reader, the table scan
  through the read unit, or the unit call.
- **A descriptor is loaded when its pipeline starts, through the transaction cache.** It is
  reloaded after any instruction that may write the catalog. That covers DDL in the query, and any
  call whose effect summary says it may write the catalog or is "unknown": embedded JavaScript, or
  a closure invoked through a value, and any unit that reaches one. A
  catalog write updates the transaction cache, so the reload sees it. A unit loads its own
  descriptors when it is called, so a unit call is always current.

## 6. Catalog facts are hints

The physical plan reads the catalog when the query is compiled (W8), but only to choose the
*shape* of the program, never to decide what is correct:
- an index it saw becomes an index-probe pipeline beside the scan pipeline, with a branch on the
  index descriptor;
- a typed layout it saw becomes descriptor-driven field reads with a branch to the generic reader;
- a unit it expects to be small may be called with a lighter calling convention.

When the program runs, the descriptors it loads decide which path runs. A hint that has gone
stale, through the query's own DDL, an aborted earlier transaction, or another client's DDL
between auto-commit transactions, makes the run take a slower path, never a wrong one.
Statistics, when they come, are more hints of the same kind.

So the program holds no catalog assumption:
- nothing is checked when a transaction opens;
- no slot is invalidated by a catalog write;
- no cached plan is invalidated by a catalog change.

Effect summaries decide correctness only in where descriptors are reloaded (§5). They also decide
fan-out placement (§11).

### 6.1 The slow paths are exercised

The slow path is not a rarely run branch. Every dereference of a record link, every `FETCH`,
every `FROM $x` and every reference cascade takes the unit path, and every untyped table takes the
generic reader. A test-only switch makes every descriptor report "no fast form", forcing every
fast instruction onto its slow path. Running the whole language suite under it catches any drift
between a fast instruction and its slow path.

## 7. Targets named by data

`FETCH`, record-link dereferences, `FROM $x`, `type::table`, graph hops to tables chosen at run
time, and reference cascades all load the target table's read or write unit by name. This
turns the `NeedUnit` read fault into a compile-cache lookup, and replaces `dispatch.rs`'s `seal` (which lowered dispatch units after
lowering), and `Reach::Any`'s listing of every table in the database.

## 8. Closures

Closures cannot be stored (`expr/src/val/closure.rs`: encoding a closure is an error). Every
closure value is made by compiled code: a closure literal in the query, or in a function's unit.
So a closure value holds a reference to code that already exists — (query program or unit, body
index, captures) — and is never compiled where it is invoked.

- A closure made in the query has its body compiled with the query, in its transaction, so its
  subqueries are planned then. That removes `DeferredSubquery`.
- A closure returned from a function holds the function unit's body index.
- A closure passed across statements of one query references the query program, which lives for
  the whole query.

Statements run on the VM, and every invocation runs the body's
unit: inside a running program the unit the run holds or links, faulting to the host for one the
run has not linked, and outside one the unit loaded for the invoking frame from the compile cache.
A unit is lowered for the invoking frame and the `VERSION` stamp it reads at; an invocation from a
computed field mid-computation runs with the record being computed, and one with a current value
and no document runs over the value as no document. No closure body is evaluated as a
`PhysicalExpr` tree (`vm/closure.rs`, `exec/compiles.rs`). A closure body is still lowered where
it is first invoked rather than with the query. A stored clause evaluated per row runs as the unit
of a closure that takes no argument (`exec/scalar_unit.rs`), and a statement a planner with no
transaction compiles is an inert `UnplannedSubquery` nothing evaluates.

## 9. Code that appears at run time: nested queries

`eval::surql`, `eval::gql` and embedded JavaScript's `surrealdb.query()` (and `surrealdb.value()`)
run query text that exists only at run time. Each runs it as a nested query: the executor
compiles the text as a query and a program of its own, in the caller's transaction and with the
caller's session, auth and options, runs it, and returns its value. The program making the call
is never recompiled. This is the one sanctioned exception in the run-time seal: the seam
`nested_query` (`exec::compiles`), and the program it compiles is sealed again by its own stream.

- **`eval::surql` and `eval::gql`** keep their gates: the function-name capability, the
  arbitrary-query subject gate and `--allow-eval-query`, which denies every subject by default.
  The text runs isolated from the call site's scope, seeing only the bindings passed with it, and
  its re-entry depth continues the caller's count toward `max_computation_depth`. Its program is
  served from the compile cache (`exec::vm::serve`) under the statement the text parses to, the
  re-entry depth and the bindings' names, for the frame of the principal's class, and only while
  the catalog it read is unchanged. It reads the bindings and the session's own values when it
  runs, so a call evaluated once per row compiles once per distinct text.
- **`surrealdb.query()`** compiles its expression through `exec::entry`, which the compile cache
  serves by text the same way.

A nested query's effects, catalog writes included, are run-time facts the logical plan cannot see.
It runs in the program's own transaction, so the program relies on the catalog it planned against
only where nothing it runs can have changed it. A statement that may run code writing the catalog —
`eval::*` (which declares `OPAQUE_BODY | WRITES_DATA`), an embedded script, a module or silo
function, a closure invoked through a value, a function body, event or field clause that reaches
one, or a schema statement in one of its expressions — is planned knowing that it may
(`CatalogWithin`):

- its own targets — the table a write writes, the source a `SELECT` reads — are loaded once, when
  it starts, and a change the code makes to them applies from the next statement on;
- it inlines no catalog definition: no function body and no `DEFINE PARAM` value is folded into
  it, and each call of a user-defined function runs the definition the transaction holds when the
  call is made, through the function's call unit, which the compile cache keys by that
  definition's version and serves only while every definition it inlined keeps the version it was
  compiled against;
- a write, or a statement holding a write or a schema statement, plans the statements written in
  its expressions late-bound, so each loads the catalog objects it names, through descriptors and
  version-keyed units, when it runs.

The statements after it in its block are planned late-bound (§4.2, §5). Its footprint is unknown,
so it holds no parallel region and runs only on the root. Apart from nested queries and the compile
cache's misses, no code reaches the VM except code compiled when the query arrived and units from
the compile cache.

**`USE NS $x DB $y`** does not need a compile. Units and descriptors are named by (ns, db, name).
Switching database only changes where they are loaded from. Inlining after a `USE` whose target is
known only at run time is not possible, so those statements call units.

## 10. Plan cache

A compiled query is cached by query text plus the session values it folded in. It holds no
catalog assumption, so it is reusable under any catalog. When the catalog has moved far from the
hints a cached program was shaped by (an index added that it has no pipeline for), it may be
recompiled when the query next arrives, never while it runs. Descriptors and decoded units are
cached per node, keyed by object version.

## 11. Fan-out: blocks over morsels

The scheduler places work items, each one a bytecode block plus the morsel (bounded key range) it
reads. The VM sees only blocks, morsels and exchanges. The host decides whether a work item runs
on another thread, a Durable Object or another machine (`EDGE_PLAN.md` rule 6).

- A block is a versioned, encoded program (W6). It reaches the catalog only through units and
  descriptors, which a fan loads from KV like the coordinator does. A fan needs no definition
  text, no planner and no compiler.
- Code that appears at run time arrives as new blocks from the compile service (§9). It is placed
  under the same rules as blocks compiled when the query arrived.
- What constrains placement is a block's effects, not how it was made. Writes that need a
  root-side check (`UNIQUE`, cross-range cascades), and statements whose reads depend on their own
  writes, stay with the root. The leaf classification decides that per block, from its derived
  effect summary.

**Streams, not buffers.** A work item writes its output into a stream as it produces it, with
backpressure, never into a local buffer collected afterwards (`EDGE_PLAN.md` rule 6). A region's
batches (`vm/region.rs`), which today run and are replayed in order through the paged merge, become
one stream per morsel, each with a fixed window, read by a merge that pulls from them.

**The fan bundle is the VM, not the engine.** A fan's start time must not dominate a fanned
query, so a fan ships the smallest wasm that runs blocks:
- the interpreter;
- the block decoder;
- the host traits (KV, exchange, scheduler, temporary store);
- the builtins and native kernels the blocks it accepts call.

It ships no parser, binder, planner or lowering. That requires:
- a block that holds no logical IR. `Arc<LogicalOp>` and `Arc<LogicalClosure>` in `Program` are
  flattened by the encoding (W6);
- a crate boundary: a VM crate that depends on no front end, with `exec/vm`'s planning and
  lowering on the other side;
- a capability manifest per block, naming the builtins and kernels it calls. A fan refuses a block
  whose manifest its bundle cannot serve, and the scheduler places that block where it can run,
  like any other placement constraint.

### 11.1 Example: `UPDATE user SET stringField = fn::my_func($this)`

Each morsel is a key range of `user`. Its block scans the range at the pinned snapshot, and for
each record:
1. calls `fn::my_func`'s unit with `$this`;
2. calls `user`'s write unit with the record and `{ stringField: v }`.

The fan's KV writes go to a segment with a key summary. `RETURN` rows flow through a concatenating
exchange. The root collects the segments and makes one commit.

It fans when two conditions hold. Both are read from derived effect summaries, never from source
text.

1. **The function's reads are disjoint from the statement's write footprint.** The footprint is
   `user` plus everything `user`'s write unit writes: its indexes, its views, and its events'
   targets.
   - A function that reads only `$this`, or other tables, answers from the snapshot exactly as a
     sequential run would.
   - A function that reads the footprint does not fan. A later record must see earlier records'
     writes (`language/statements/update/set_subquery_sees_earlier_writes.surql`).
2. **The write unit's effects are local to the morsel, or mergeable.**

   | Effect | Fans | How |
   |---|---|---|
   | Non-unique index | Yes | The entry key holds the record id, so morsels write disjoint keys |
   | `UNIQUE` index | See below | Each fan checks the snapshot; the root rejects one key claimed by two segments |
   | Count index, aggregate view | Yes | Fans emit partial deltas; the root merges them |
   | Event writing a per-record target | Yes | Local to the morsel |
   | Event writing a shared record | Only as a mergeable delta | Otherwise root-side |
   | Changefeed | Yes | The root assigns versionstamps at commit |
   | Clock or entropy in the function | Yes | Fixed per (query, partition), so a retried producer is identical |

**`UNIQUE` and order.** Run sequentially, a statement that moves a unique value from one record to
another succeeds or fails depending on the order it visits them. A fanned run has no order.
Either the uniqueness check is defined as holding at the end of the statement, which is
deterministic, or a statement that may rewrite a `UNIQUE` field does not fan. This is a language
decision.

**Required of the unit compiler:** effect summaries carry per-table read and write footprints,
not only flags (`EffectSet` today holds `WRITES_DATA`, `OPAQUE_BODY` and similar). A write unit's
summary names every table its indexes, views and events write.

## 12. What gets deleted

- `Control::Deferred`, `BlockInstr::RunDeferred`, `vm/deferred.rs`.
- `DeferredSubquery` (`physical_expr/subquery.rs`), and transaction-less planners for closure
  literals, permission predicates and field state.
- `vm/fault.rs`'s compile paths and closure relinking (`closure.rs` `relink`, `fault_body`).
  `NeedUnit` remains as the request for a unit, served from the compile cache.
- `dispatch.rs` `seal`/`seal_knowing`.
- The per-statement `plan_statement_inner` in the executor, replaced by one compile per query.
- `enforce_row`'s and `fetch_record`'s run-time `build_field_state`.
- The scattered staleness checks (`def_hash`, catalog stamps): nothing holds a stale copy.
- The speculative overlay's use in planning.

## 13. Alternatives considered

- **Two variants per statement** (a specialised one, and a generic one chosen by a statement-level
  version guard). Rejected: one changed table discards the whole statement's specialisation,
  every raceable statement carries its code twice, and the compiler must model the query's DDL.
- **Per-instruction version guards.** Each guarded instruction checks a version when it runs.
  Rejected: every guard is work on the hot path, and it still bakes catalog facts into the
  program.
- **Premises.** Inline catalog code into the query against the snapshot, check (object, version)
  premises once per transaction, have catalog writes clear premise slots, and branch each region
  to a non-inlined form. Rejected: under snapshot isolation, with the catalog pinned per query
  (§4.2) and run-time query text confined to nested queries (§9), only the query's own DDL can
  change what it inlined, and the logical plan sees that DDL in the text. No run-time mechanism is needed.
- **Run-time query text as block requests to the compile service.** Deferred: `eval::*` and
  `surrealdb.query()` are nested queries (§9), whose compiled programs the compile cache already
  serves by text.
- **Running `eval::surql` only on the coordinator, as a placement bar.** Rejected: it splits
  placement by how code was made rather than by its effects. Blocks from the compile service,
  placed like any other block, need no exception.
- **Refusing `eval::*`.** Rejected: it shipped in 3.3.0, behind `--allow-eval-query`, and a stored
  body that calls it must keep working. It runs as a nested query (§9).
- **Compile per transaction.** This removes the other-client problem without a fallback.
  Rejected: it compiles while the query runs, and a `BEGIN`…`COMMIT` group with conditional DDL
  still needs something like deferral.
- **One variant per combination of outcomes.** That is 2ⁿ variants over n DDL-bearing
  transactions, and it still cannot predict other clients' DDL.
- **Recompile when the catalog changes under a running program.** Rejected: it compiles at run
  time, which is the rule this RFC exists to keep.
- **Inline across catalog objects in units.** Faster unit calls. Rejected: a function change would
  invalidate every unit that inlined it.
- **Store units in the catalog, compiled by the DDL, in its transaction.** No compile after a
  query arrives. Rejected:
  - units become a persisted format, needing a cross-build ABI, decoding of older ISAs, a
    migration on upgrade, and upgrade tests;
  - a unit compiled by an old build keeps that build's lowering bugs until it is migrated;
  - a schema import recompiles a table's units once per DDL statement.

  A cache keyed by build serves units with none of these costs.
- **Node-local units only, with no shared tier.** Rejected: every node and every fan compiles every
  unit on first use, and fans would need the compiler.

## 14. Costs and risks

- **First-touch latency.** A cold miss compiles a unit on demand: once per (object, version, build)
  across the cluster, then shared. Requesting a query's named units when it arrives moves most
  misses before the program starts. A warm-up pass after an upgrade or a DDL removes the rest.
- **Principal-generic units are slower than today's principal-specialised ones.** Permission
  branches are evaluated per call.
- **A pinned catalog per query.** A query spanning several auto-commit transactions does not see
  another client's DDL committed between them (§4.2). This is a semantic choice, and it is
  documented.
- **Code size.** A fast instruction carries its slow-path branch. An access path holds an index
  pipeline and a scan pipeline.
- **Effect summaries must be sound.** A unit that may write the catalog but whose summary says it
  does not would leave a stale descriptor in a register (§5). Summaries are derived by the
  compiler, never declared, and "unknown" is the default for anything it cannot analyse.
- **Descriptor-driven reads versus constant-folded reads.** Reading a slot through a register is
  one indirection more than a baked offset. Measure it on the scan benches against today's
  specialised scans before deleting them.
- **Cache growth.** Entries for superseded versions and retired builds are garbage, collected by
  version and build (§3.4).
- **Correctness of the slow paths.** The forced slow-path suite run (§6) is the check.

## 15. Open decisions

1. **The block encoding.** Blocks are encoded for the shared cache and for shipping to fans (W6).
   Within one build it is free to change, since entries are keyed by build. Proposed: the
   versioned `Program` encoding, behind an encode/decode interface. No layer below SurrealQL is a
   stable format.
2. **Access-method units.** Authentication runs before a session has a database selected in some
   paths. Are signin and signup bodies units like the rest?
3. **Index choice by the read unit.** How rich is the conjunct form the query hands the read
   unit: single-field equality and ranges to start, or compound and full-text from the start?
4. **Warm-up policy.** Does a node warm its cache over the whole catalog before taking traffic,
   after an upgrade, or only on demand? Does a DDL commit trigger a warm compile of what it
   invalidated?
5. **Unit calling convention.** How light can a unit call be (arguments in the caller's
   registers, no frame allocation), given that every user function call in a query is one?
6. **`UNIQUE` under fan-out.** Is uniqueness checked at the end of a statement, so a fanned write
   is deterministic, or does a statement that may rewrite a `UNIQUE` field never fan (§11.1)?

## 16. Order of work

1. Catalog versions for every object kind (§3.7).
2. The block encoding (W6).
3. The unit compiler: the lowering routines for table reads and writes (with index maintainers,
   events and views), functions and params, as pure functions of one definition,
   principal-generic, with derived effect summaries and per-table footprints.
4. The compile service and the two-tier compile cache keyed by (object, version, build), with the
   query's named units requested when it arrives.
5. Descriptor loads, descriptor-parameterised fast instructions and
   slow-path branches. Measured against today's specialised scans and writes.
6. Targets named by data go through units from the compile cache. Delete `seal` and `Reach::Any`.
7. Closures reference compiled bodies. Delete `DeferredSubquery` and closure relinking.
8. One compile per query against the catalog pinned at arrival: the logical plan inlines catalog
   definitions except objects the query's own DDL may touch, which are unit calls, and catalog
   facts are hints to the physical plan. Descriptor reloads after
   catalog-writing calls. Delete `RunDeferred` and per-statement planning.
9. The forced-slow-path suite run in CI.
10. `eval::*` and `surrealdb.query()` run through the `nested_query` seam, their programs served
    from the compile cache by text.
