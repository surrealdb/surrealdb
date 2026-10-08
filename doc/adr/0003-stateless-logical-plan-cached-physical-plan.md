# ADR 0003: A stateless logical plan and a catalog-bound, cached physical plan

**Status:** Accepted · **Scope:** `surrealdb/core/src/exec/logical/`, `surrealdb/core/src/exec/planner/`,
`surrealdb/expr/src/expr/logical/`

## Context

A statement is planned in two stages. The **logical plan** rewrites the statement into an
equivalent one that asks for less work. The **physical plan** decides how that work is done:
which index answers a predicate, whether a sort runs in memory or spills, which layout a scan
reads, whether a subquery is evaluated once or per row.

What each stage may read decides three things: which rewrites each stage can express, whether
either plan can be cached, and whether a rewrite that is only sound for one schema can be written
where it applies to every schema.

`doc/executor/AST_TO_EXECUTION.md` takes the other position: the catalog enters the logical plan.
`bind` resolves table and field permissions and computed fields into plan nodes before the rules
run, so the rules see them as ordinary predicates and expressions, and the bound plan is valid for
one transaction and one principal and is never a cache key. This ADR replaces that decision. The
reasons are below.

## Decision

### 1. The logical plan is stateless

The logical plan is a function of the statement text and nothing else. Lowering and every logical
rule read no catalog, no statistics, no storage, no session and no principal. The same statement
produces the same logical plan on every node, in every database, for every caller.

- The logical plan's types carry no resolved state. A source names a table; it has no schema, no
  permission node above it and no computed-field layers. A rule cannot read what the type does not
  hold, so a rule that needs the catalog cannot be written as a logical rule by mistake.
- A rewrite is logical only when it is sound under **every** catalog the statement could be
  planned against. "Sound for the schemas we tested" is not logical.
- Session state is state. What the caller's capabilities allow, and the bodies of `DEFINE FUNCTION`
  and `DEFINE PARAM`, are not visible to the logical plan. A rewrite that turns on them — inlining
  a user function, folding a call the capabilities may forbid — is physical.

### 2. The physical plan is catalog-bound

The physical plan is built from the logical plan, the catalog and statistics. **Resolution is its
first stage**: tables, fields and their kinds, indexes, permissions, `VALUE` and `ASSERT` clauses,
events, table views, references, functions, parameters and storage layout. After resolution the
physical plan may assume anything the catalog states — that an index exists and covers a field,
that a field is declared `int`, that a table is laid out as PAX, that no event on `person` writes
`company` — and its rules choose between alternatives by cost.

The physical plan is inert. It describes execution and does not perform it, and it keeps its
per-node properties — access mode, delivered ordering, partitioning, cardinality — for the rules
that read them.

### 3. The physical plan is cached, and what it assumed is its key

A physical plan is reused for as long as everything it assumed still holds. Its assumptions come
in two kinds, and they are not treated alike:

| assumption | examples | effect when it changes |
|---|---|---|
| **Catalog** | an index exists; a field's kind; a table's permissions, events, layout; a function's body | the plan may be **wrong**. It is invalid and is re-planned |
| **Statistics** | table size, selectivity, distinct counts | the plan may be **slow**. It stays correct and is re-planned when the estimates drift |

From this:

- **Every catalog read made while planning is recorded as a dependency**, with the version it saw:
  the table's `CatalogVersion` for a stamped table, and the content hash of the definition for one
  no catalog write has stamped. A cached plan is served only when every dependency is still at the
  version recorded. A missed dependency is a correctness bug, not a performance bug, so recording
  happens where the catalog is read, not in a list maintained beside it.
- **Correctness never rests on statistics.** A rule may use statistics to choose between plans
  that give the same answer. It may never use them to choose a plan that is correct only while the
  statistics are accurate.
- **The principal enters the key as a permission regime, not an identity.** Whether permissions
  apply at all (a root or owner bypasses them; a record user does not) decides what resolution
  emits, so it is part of the key. `$auth`, `$session`, `$token` and the caller's identity are
  parameters of the plan, evaluated when it runs, and never baked into it. Two record users of the
  same access method share a plan.
- **A transaction that has run uncommitted DDL plans privately.** Its catalog versions exist only
  inside it, so nothing it plans enters the shared cache.
- **A plan whose table is chosen at run time** (`FROM $table`, `type::table(…)`) records that the
  table is dynamic. The choice is made when it runs, not when it is planned, and its catalog is read
  then.
- **Serving a cached plan still needs a snapshot to check its dependencies against:** one version
  read per table it depends on, in the transaction that runs it. What caching saves is the
  planning, not the transaction.

The logical plan is cacheable independently, by statement text alone, because it depends on
nothing else.

### 4. The existing planner becomes the physical layer; it is not replaced

`exec/planner/` already makes the physical decisions — access-path choice, clause collection, fast
paths, scan enforcement — and holds semantics that are not written down anywhere else. The physical
plan is produced by splitting that planner into **decide**, which emits the physical plan, and
**build**, which turns the physical plan into operators. There is no second planner beside it and
no fallback between two planners.

The split is checked by equivalence: for every statement in the language suite, the operators built
from the physical plan are the operators the planner builds today. The suite must pass in its
default and strict modes before and after.

## Consequences

- `exec/logical/resolve.rs` moves into the physical stage, and resolved state leaves the logical
  plan's types (`LogicalSource::schema`, `PermissionFilter`, the projection layers).
- The logical rules that read resolved state move to the physical stage or split into a catalog-free
  part and a physical part. At the time of writing that is every rule except `control_flow`, and
  `RuleContext`'s catalog- and session-derived fields go with them.
- Rewrites that look logical but are not are recorded as physical. Two are pinned by language tests
  so that no logical rule can reintroduce them:
  - Hoisting an invariant subquery out of a data clause. A subquery in `SET` is evaluated per
    subject and sees the statement's earlier writes, including what events wrote elsewhere
    (`language/statements/update/set_subquery_sees_earlier_writes.surql`). Hoisting is sound only
    when the subquery's reads are disjoint from the write's whole footprint, which is catalog state.
  - Dropping `SET a = a`. The read of `a` goes through the caller's field permissions
    (`language/statements/update/set_field_to_itself.surql`).
- The rules that `AST_TO_EXECUTION.md` puts after binding — a predicate moved through a projection
  but not a permission layer, index selection offered through a chain of permission nodes — keep
  their behaviour. They run in the physical stage, over the same nodes.
- The bound plan stops being the per-transaction object. The cached physical plan is, together with
  the parameters it runs with.

## Open

- **Parameter values.** A parameter's value can be baked into the physical plan, which gives better
  plans and makes the key value-specific, or kept as a hole. Whichever is chosen, a parameter whose
  value decides a catalog-dependent choice — a table name, an index hint — is part of the key or is
  resolved at run time.
- **Re-planning on statistics drift.** The threshold, and whether a re-plan is synchronous or
  happens in the background while the old plan keeps serving.
- **Cache scope.** Per node or shared across nodes. Shared requires the physical plan to be
  serialisable and its function references stable across nodes.
