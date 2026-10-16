# From the surface AST to execution

How a statement gets from the parser to a running program, what runs it, and
where the catalog enters the plan. The contracts live in the
module docs named below; this document is the map between them, and the plan
for where the logical layer goes next ([The target algebra](#the-target-algebra),
[The steps](#the-steps)).

## The principle

**One engine runs every statement: the bytecode VM** (ADR 0007). Its input is
`LogicalOp`, a tree of operators whose scalars are `LogicalExpr`. `expr::Expr`
is the stored and wire form of a statement and is never executed: nothing
lowers from it to run a statement, nothing adapts a value that may be "in
either form", and no statement is rebuilt from a plan. A statement the VM does
not lower is an error; no other executor takes it.

Two consequences shape everything below:

- **One lowering.** Lowering takes the surface AST at a statement boundary.
  There is no choice of engine to make, and no second encoding of a statement
  between the parser and the VM.
- **The catalog enters the logical plan.** Table permissions, field
  permissions and computed fields are nodes in the bound plan, so
  optimisation sees them as what they are — predicates and expressions — and
  the physical planner compiles one shape whether a node came from the
  statement or from the schema.
- **The plan is a relational algebra over documents, and nothing else.** A
  path is an expression, a derived field is a projection, and a relation
  reached from a row is a join. The layers below describe the plan as it is;
  where it still departs from this — a relation reached from a row is
  evaluated per row rather than joined — the target algebra says what
  replaces it.

## The shape

```text
sql::Ast ─ per statement, in the executor
│
├─ BEGIN / COMMIT / CANCEL / OPTION ──────────────────────── the driver
├─ Expr(e) ─ lower(e) ─ LogicalOp ─ bind ─ map ─ optimize ─ lower ─ Program ─ VM
│                │
│          NoLogicalForm / unsupported ─────────────────── error
│
GQL text ─ parse_gql ─ LogicalOp::Match ─────── bind ─ map ─ optimize ─ lower ─ Program ─ VM
```

`USE`, `LIVE`, `KILL`, `SHOW`, `ACCESS` and every schema statement are
statements like any other: each lowers to a `LogicalOp` and runs as VM
instructions. Only the transaction statements (`BEGIN`, `COMMIT`, `CANCEL`) and
`OPTION` are the driver's.

**A statement the VM cannot run is an error.** Planning has two points at which
a statement can be refused, and each fails the statement with an error that
names the construct:

1. **Lowering** (`surrealdb_expr::expr::logical::lower`). Pure, and needs no
   catalog. A statement holding a shape lowering does not express is
   `NoLogicalForm`, and fails without touching the transaction.
2. **Binding and lowering to a program** (`exec/logical`, `exec/vm`). Needs the
   catalog, because this is where permissions and computed fields enter the
   plan. A plan node or an expression the VM has no instructions for fails the
   statement, and so does a block holding such a statement: a block is
   compiled in whole when the query arrives. A stored clause with no logical
   form is not a refusal: the scan evaluates it itself (below).

A statement boundary is wherever text becomes a statement: the executor for a
query, and `eval::surql` for the statement it parses. Both make the same
choice the same way, and neither falls back: inside a plan there is no
evaluator other than the VM's. An operator of `exec/operators/` is a host
service a program calls (a storage scan, an index iterator, a record fetch),
never a second executor.

A GQL `MATCH` is a plan from the start and has no statement behind it:
`plan_match` (`exec/planner/match_plan.rs`) compiles it whole, or the query
fails. The plan is not passed through `bind` as a unit; each anchor table
scan inside it is a `Filter` over a table `Source` that `plan_match` binds
and compiles as a plan of its own, so the catalog enters a `MATCH` plan per
scan rather than per statement.

## The layers

### Lowering — `surrealdb_expr::expr::logical`

`lower(sql::Expr) -> Result<LogicalOp, NoLogicalForm>` and
`lower_expr(sql::Expr) -> Result<LogicalExpr, NoLogicalForm>`. A `SELECT`
becomes its relational pipeline; every other streaming statement kind (`LET`,
`RETURN`, `FOR`, blocks, `IF`, `INFO`, `SLEEP`, `EXPLAIN`, a bare expression)
becomes its statement operator. A statement in scalar position is
`LogicalExpr::Subquery(Box<LogicalOp>)` — the one embedding, so a statement is
never re-encoded as a scalar variant of its own. A block in scalar position is
`Subquery(Sequence)`; a block as a statement is `Sequence`.

`LogicalExpr` holds scalars only: literals with their nesting flattened,
record ids, parameters, paths as compositions of expression forms (`Field`,
`Access`, `Index`, `Each`, `Broadcast`, … — [Idioms are
expressions](#idioms-are-expressions)), constants, tables, operators, calls,
closures, `IF` as a value, and the subquery. A
`FROM` item is a `SourceRef { Table, RecordId, Mock, Param, Dynamic }` on a
`Source` node rather than a scalar: a source is not a value, and choosing an
access path is the physical planner's job. Every scalar renders as SurrealQL
through `ToSql`; a rendered plan is deterministic and golden-testable, and
nothing needs it to reparse.

The unbound plan is pure and identical for every caller, so it can be cached
by statement text.

### Binding — `exec/logical`

`bind(op, &BindContext)` turns the unbound plan into the plan of one
transaction and one principal. The passes run in this order, each a
synchronous rewrite that gathers what it needs with a read-only walk, awaits
the lookups, then rewrites:

| Pass | What it does |
|---|---|
| `resolve` | Each table source gets its schema. The table's `PERMISSIONS FOR select` becomes a `PermissionFilter` above it, and the `COMPUTED` fields and field `PERMISSIONS FOR select` a stack of projection layers above that — `Project[*, body AS c : kind @computed]`, `Project[*, IF p THEN f ELSE NONE END AS f @permission]` — in the order the scan pipeline applies them ([The row is a document](#the-row-is-a-document-and-a-projection-shadows)). Every user expression above the stack reads plain field names; nothing is rewritten. |
| `params` | Statement-scoped `$param`s become literals. |

Nothing else rewrites the plan before the binder reads it. The rewrites a
statement is owed run on the IR the binder builds from it: the binder writes
each membership of a filter that runs once as a join in its canonical form
(`exec/ir/build/membership/lift.rs`), binds the body of a user-defined
function beneath its call (`exec/ir/build/inline.rs`), and the loop
optimiser's passes (`exec/ir/passes`) fold row-independent subexpressions,
decide `IF`s and filters, move a filter into the subquery it reads and beneath
a layer chain, drop the layer columns nothing reads, run a function body in
the caller, and share and hoist what is evaluated more than once.

Every node has a `Schema { fields, openness }`: `Source` from the catalog,
every other node derived from its inputs. A `SCHEMAFULL` table is closed, a
`SCHEMALESS` table, a literal and every statement kind open. Typing is
partial — `LogicalExpr::kind(&Schema)` answers in the vocabulary of `DEFINE
FIELD … TYPE`, and `Kind::Any` is the honest answer for an *undeclared*
field, a parameter, a call or a subquery; a declared field carries its
declared kind whether the table is `SCHEMAFULL` or `SCHEMALESS`. Above a layer
a field is typed as the layer's column: a computed field by its declared kind,
a guarded field as `option<kind>` of the field it guards. An optimisation
fires on a known kind and never on `Any`. A kind is declared or checked:
`FieldSchema::checked` marks a kind a value is known to have — a column a
layer coerces as it lands — against one a stored field is only declared to
have, since a declaration is enforced on write alone. An optimisation that
trusts a declaration reads `kind`; one that drops a check or a conversion
reads `LogicalExpr::checked_kind`.

The bound plan is valid only for the transaction and the principal it was
bound for, and is never a cache key. A closure body is the one part of a plan
binding leaves as lowered: the planner compiles it with no transaction,
namespace, database, principal or `VERSION` of the creating statement, so a
closure value carries nothing of that statement beyond its captures and
resolves tables, permissions, indexes, functions and parameters where it is
invoked.

**Catalog access.** The analyzer reads the catalog at bind time because that
is where permissions and computed fields enter the plan — before
optimisation, not at scan time. It reads current-version tables from the
node-wide `CatalogSnapshot` (`kvs/cache/ds`), one `TableSnapshot` per table,
keyed by `(namespace, database, table, catalog version)`. The version is a
per-table uuid (`!cv{tb}`, `CatalogVersion`) rewritten by every provider
method that writes the table, one of its fields or one of its indexes, and
is read per table through the transaction, so one read validates that
table's snapshot: a committed DDL elsewhere changes the version and misses,
a transaction's own uncommitted DDL mints a version only it can see, a
rolled-back version is never served, and a write to one table leaves every
other table's snapshot in place. Version identity rests on every DDL
statement reaching the table through `replace_tb`'s locked read of the
table key; the version write itself is blind. A table no catalog write has
stamped has no version and is shared under the content hash of its
definition instead, which every write to its fields or indexes rewrites
(`Lookup::CatalogUnstamped`). `VERSION`ed reads go to the transaction. Stored bodies are
canonical text, compiled through the one funnel `SurqlText::compile` with
`SurqlTarget` implemented for `LogicalExpr` and `LogicalOp`
(`catalog/src/text.rs`); no `expr::Expr` is built on the way, and a
definition carries each body compiled once for its lifetime
(`FieldDefinition::{computed_body, select_body}`,
`TableDefinition::permissions_body`), so binding a table does not re-lower its
clauses per statement. A body with no logical form leaves every source of
its table unresolved (`schema: None`, no analyzer node above it); a catalog
read error is the error it is. Expansion is bounded: a table's bodies are
expanded once, and a source of a table already being expanded is left
unresolved, which the physical planner compiles with runtime enforcement.

### The physical planner — `exec/planner`

The operators this planner builds are the host services the VM calls, not an
executor (ADR 0007): a program requests a scan, an index read or a record fetch
through a host step, and the host runs the operator that serves it. A statement
is run by the program `exec/vm` lowers from the bound plan.

A relational tree is read by walking down from its top (`collect_pipeline`,
`planner/fold.rs`), collecting clauses into a `PipelineClauses` until a leaf or
a node the walk does not collect. The VM's mapping (`vm/map.rs`) then maps the
leaf with the whole clause set in hand, choosing its access path through the
machinery in `planner/select` (fast paths included), and puts the clauses back
on in pipeline order. A source nested in `FROM` is mapped by recursing into its
plan, never by rebuilding a statement.

**The barrier rule.** A user expression never moves below a
`PermissionFilter`, and below a layer only when it reads no column of that
layer — a column whose frame is not the caller's is opaque — and neither node
is dropped. The `layers` rule is what moves one: the conjuncts that read no
layer column, by name or opaquely, stand in a filter directly above the
source, or above the chain's topmost permission node. A clause collected
above such a node applies to the node's output in full. The one permitted
crossing is access-path choice: a chain of `PermissionFilter`, layer and
pushed-filter nodes directly over a table source is planned with the chain's
filter, and the pipeline's predicate and ordering, offered to index selection
exactly as a bare leaf is, because an index lookup reads keys and the nodes
above still judge every row it yields. `planner/fold.rs`'s `chain_offer`
states what the offer withholds of the pipeline's predicate: every conjunct
that reads a layer column — by the field's name, or opaquely through a row
parameter, the document or a nested plan — the ordering when one of its keys
does, and a permission predicate never joins a KNN residual. A permission
predicate joins the offer only where no part of the offer is evaluated per row,
and the table permission is judged before the user's predicate, so a filter
beneath a table permission is judged where it stands and never by the scan.

**A vertical cut and a horizontal one.** The barrier rule governs what crosses
the chain, not how many chains a table is read by. Replicating the whole chain
over a partition of the key range — one chain per disjoint range, their results
concatenated — judges every row exactly once, by the same nodes, in the same
order, so it answers what the single chain answers. A permissioned table is
therefore not unshardable: what the rule forbids is moving an operator *through*
the chain, not running the chain more than once. What does not follow the
partition is any clause whose answer counts rows the chain has already judged.
A `LIMIT`, a `START` and an ordering stand above the union of the ranges, never
inside one, exactly as they stand above the permission node and never inside the
chain; an aggregate follows the partition only where its parts compose, which a
count does and a mean does not. The partition has to be of the source's own key
range and disjoint — a row read by two ranges is judged twice, and a chain whose
source is not a key-ordered scan has no such partition to cut along.
`reproductions/a_permission_chain_partitions_by_key_range.surql` pins the parity
for a chain that drops rows, cuts fields and computes a field.

**Scan enforcement.** A `Source` with a resolved schema is planned in
`EnforcementMode::Planned` (`operators/scan/resolved.rs`): the analyzer put
above it everything the read needs, and the scan applies nothing again — so
a field guard sees the raw row and a computed field is computed once. The
layers map to `Plan::Layer`, which the VM lowers to instructions that evaluate
its columns on stored fields against the input row and its columns on computed
fields against the reduced record — the input with the stored guards applied
— each in the column's frame, and land each under the field's name: a
computed column is inserted whatever its value; a guard that does not hold
cuts the field; one that holds sets the admitted value, except that a `NONE`
admitted for a stored field leaves the row as it is. A guard is judged
against the input row with `$value` bound to the value its column evaluated.
A source left unresolved is planned in `EnforcementMode::Runtime`, where the
scan resolves the table's permissions and field state itself, as it does for
a source the analyzer could not reach. A stored clause is lowered
from its stored text there, in the frame its caller is in, so every clause the
catalog holds is enforced; one that cannot be lowered fails the statement that
needs it.

**A source that yields values.** A `FROM` expression that is not a table
yields a value: an array is that many rows, `NONE` and `NULL` are no rows,
anything else is one row. A row that is a record id is the record it names,
read with the caller's `SELECT` permission and its computed fields evaluated,
so an id no record answers is no row at all. The source performs that read
before any predicate, limit or start the planner pushed into it, so a pushed
predicate is judged against the record and never against a row the source does
not produce. The VM's values source (`Source::Values`) does this for every
expression alike: which form plans a given expression is a syntactic
distinction the query does not make.

`Planner::scalar_metadata` describes a `LogicalExpr`, and `Clause::of` makes the
`Clause` that evaluates it; `compile_unit_expr` is the one
runtime re-entry, used where a stored clause is compiled by a scan that
resolves its table when it runs (a `DEFINE PARAM` permission, a function's
`PERMISSIONS` guard); a dynamic source resolved per row builds a
transaction-less `Planner` of its own.

A call to a `DEFINE FUNCTION` the inlining pass did not inline is compiled with its
body: for each call site, `Planner::plan_function_body` reads the function's
snapshot from the node-level cache, binds the body for the caller's principal
capped by the function's `AUTH LIMIT` with the arguments as per-call names,
plans it one re-entry deeper, and lowers it as a unit of the program with the
definition's content hash. A call runs the unit while the definition it reads
still hashes the same, and fails closed otherwise; a recursive edge, a call to a
function whose body is being compiled, is bounded by the runtime's depth count.

## What stays `expr::Expr`

`expr::Expr` is the stored and wire form of a statement and is never run. It
remains in the `SerializeRevisioned` encoding, whose text is SurrealQL; in
`INFO FOR …` rendering; in the catalog idioms `exec/logical/resolve.rs` reads
from field definitions; and in `exec/field_path_convert.rs`. A caller outside a
plan that needs one expression evaluated — a schema statement's names and field
paths, an `ACCESS` subject, a bucket's `PERMISSIONS` clause, a record access's
`SIGNIN` — builds the VM's frame with `exec::entry::frame` and evaluates it with
`exec::entry::scalar`; the pair is the one route in, so a clause is judged
the same way whichever caller reaches it. A GQL `MATCH` plan holds no
`expr::Expr`: its scalars are `LogicalExpr`, described through
`Planner::scalar_metadata` like every other scalar, and `operators/mutate.rs`
builds the synthetic write statements the binder binds with the plan; a mutation
step runs each as a group of its program, with the values it has already
evaluated bound as parameters.

## The target algebra

Three tenets make the plan a relational algebra over documents: a path is an
expression, a derived field is a projection, and a relation reached from a
row is a join. The first two are the plan as it is in full. The third is the
plan as it is for the correlated membership subquery; the shapes the
decorrelation rule does not lift — `FETCH`, a record-link dereference, a
graph lookup, a scalar subquery in value position — are evaluated one row at
a time where they could be a join.

### Idioms are expressions

`a.b[0].c`, `arr[WHERE x > 1]`, `a.len()`, `{ x, y }` destructuring and
`->likes->post` are compositions of ordinary expression forms, not steps of a
path walked by a dedicated evaluator:

| Idiom part | Expression form |
|---|---|
| `a` (root) | `Field(a)`: a read of the row's field |
| `v.b` | `Access { base, field }` |
| `v[i]`, `v[0]`, `v[$]` | `Index { base, index }`, with `First` / `Last` as index values |
| `v.*` | `Each(base)`: the values of an object, the elements of an array |
| `v…` | `Flatten(base)` |
| `v[WHERE p]` | `Filter { base, predicate }`: `p` sees the element as `$this` and the enclosing row as `$parent` |
| `v?.b` | `Optional(base)`, a null-propagating access |
| `v.{ x, y }` | an `Object` of accesses |
| `v.len(args)` | `Method { base, name, args }`: a method dispatches on its receiver's type at run time, so it stays one form rather than a `Call` |
| `(e).b` | `e`, then `Access` |
| `->e[WHERE c]->n` | `Subquery` of the `SELECT` the lookup is: `SELECT VALUE out FROM e WHERE in = $parent.id AND c`, then the next hop |
| `.{1..3}(…)` | `Recurse { base, range, path, instruction }`: a fixpoint over a path, kept as one form because it is not a step |
| (no syntax) | `Broadcast { base, body }` and `Current`: the array-continuation rule made explicit, below |

A path has one rule an expression tree does not carry on its own: a field
read, `.*` or destructure applied to an array applies *the whole rest of the
path* to each element, so `[{a: [1, 2]}, {a: [3, 4]}].a[0]` is `[1, 3]` and
not `[1, 2]`. Lowering makes that explicit: a mapping step followed by a
non-mapping step lowers to `Broadcast { base, body }`, whose body is the step
and the rest applied to `Current`, the element. A graph hop over an array that
did not come from a hop broadcasts the same way, and a hop followed by a hop or
a filter is flattened between them. These rules are static functions of the
sequence of steps, and are the only place path semantics live once the idiom
evaluator is gone. A body's `Current` renders as nothing, so a lowered path
renders as the idiom it came from.

Everything that is a key becomes an expression: `Split`, `Aggregate.group`
and `LogicalOrder.value` take `LogicalExpr`. The catalog keeps a declared path
type for schemas — `DEFINE FIELD arr.*.secret` names a position inside a
value, which is a type-level statement — and `Schema` types an expression by a
partial map from `Field`/`Access`/`Each` chains to declared paths. Index
analysis uses the same map to match an expression to an indexed column; a
shape the map does not cover matches no index, as today.

The physical consequence is that `physical_idiom` (`planner/idiom.rs`) and its
constant-chain fold go: each form compiles to its own `Clause`, and constant
folding is the `fold` pass's job.

### The row is a document, and a projection shadows

A computed field and a field permission are columns of a `Project`, under the
field's own name:

```text
Project[*, body AS c : kind @computed]                 -- COMPUTED c
Project[*, (IF p THEN f ELSE NONE END) AS f @permission] -- PERMISSIONS FOR select WHERE p on f
```

`*` copies the input row; a column named `c` shadows a stored `c`. Every
operator above reads the projected row, so `SELECT *`, `$this`, a projection
function, `GROUP BY c`, `SPLIT ON c`, `ORDER BY c` and a dynamic index all see
the derived value with no rewriting of reads. Such a `Project` — `*` first,
then columns whose every frame is not the caller's, no `OMIT` — is a *layer*
(`LogicalOp::is_layer`); a `Project` that is not a layer is the user's. The
runtime's read order — table permission, computed fields, field guards,
re-derivation of a computed field that read a now-hidden field, the computed
fields' own guards — is a stack of layers, each reading the layer beneath it
(`exec/logical/resolve.rs` states the stack rule by rule):

```text
Project[*, IF p_c THEN R_c ELSE NONE END AS c @permission]  -- computed fields' guards; R_c the
                                                             -- body, evaluated against the reduced
                                                             -- record, when it read a guarded field
Project[*, IF p_f THEN f ELSE NONE END AS f @permission]    -- stored fields' guards
Project[*, body_c AS c : kind @computed]                     -- computed fields, one layer per
                                                             -- rank of their dependency order
PermissionFilter[p_table]
Source[t]
```

A column carries the `EvalFrame` its expression evaluates in and the kind its
value is coerced to; the frame stays on the operator's slot, never on the
scalar. A nested or wildcard guard (`a.b`, `arr.*.secret`) is a column that
rebuilds the containing value with the guarded position replaced by its
guard: `LogicalExpr::SetField { base, field, value }` sets one key of an
object (a `NONE` removes it), and `Broadcast` maps it over the elements of an
array. A guard is a column only where its field can reach the caller — the
statement reads the field or the row as a whole, or a re-derivation reads it
— so a count over a guarded table judges nothing and keeps the key-range
count. Reading the row as a whole is what a statement does when it cannot be
told what it reads: through `$this` / `$self` / `$parent` or `@`, a nested
plan, a call evaluated against the row (a user-defined function, a script, a
module function, a projection function), or the invocation of a value whose
body the expression does not hold — `$pred()`, `$o.check()`, an unregistered
method, which calls a closure stored under that name on the receiver
(`exec/logical/reads.rs::invokes_an_unseen_body`). A closure reached at
runtime reads whatever row it is invoked against, so an invocation read as
demanding nothing would leave a denied field unguarded and judge the stored
value. A source the resolver leaves without a schema is planned in
`EnforcementMode::Runtime`; `exec/logical/resolve.rs` lists what leaves it
so: a stored clause with no logical form; a cyclic computed dependency order;
and, for a checked caller, a guard whose `$value` reaches a nested plan, a
guard path ending in `*` or mapping over arrays at two positions, a `*` over
a position that may hold an object, guards under one root running under
different `AUTH LIMIT`s, and a re-derived body reading the row as a whole.
Those are not shapes of the plan.

The barrier rule is the standard pushdown rule with one clause. A filter
moves below a `Project` by substituting the columns it reads; a column whose
frame is not the caller's is opaque to substitution, and so is a
caller-frame column projected as anything but a path, a literal, a parameter,
a constant or a comparison over those — substituting a call or an arithmetic
expression would evaluate it once per mention, and no index answers it — so a
predicate on a guarded or computed field stays above the layer that produces
it and never drives an index. That is what "a layer column matches no index" says, stated
as a property of projections rather than of a special node. A user expression
never moves below a `PermissionFilter`, and access-path choice is the one
permitted crossing.

### A relation reached from a row is a join

Four things read another relation keyed by a value of the row: a correlated
subquery (`$auth.id IN (SELECT VALUE user FROM member WHERE org =
$parent.org)`), `FETCH`, a record-link dereference (`author.name`, a point
get per row inside the path evaluator) and a graph lookup (`->likes->post`).
Each has the form of a join:

```text
Join {
    inputs: [left, right],
    kind: Inner | Left | Semi | Anti,
    on: Vec<(LogicalExpr, LogicalExpr)>,   -- equi-keys, left side then right side
    filter: Option<LogicalExpr>,           -- the residual, over both sides
    alias: RelationAlias,                  -- how the right side is named on the row
    frame: EvalFrame,                      -- the caller's, or the permission's
}
```

The right side is named by a relation alias the pass mints (`_join0`,
`_join1`, …): a top-level key of the output row holding the matched record,
the convention MATCH binding rows already use. It is a name for another
relation, not another name for a field of the same row, and the pipeline's
final `Project` drops it as it drops every column the statement did not ask
for. `Semi` and `Anti` add nothing to the row. The join's schema is the left
schema with `alias` as a field whose kind is the right side's record, `null |
record` for a `Left` join, and `Any` when the right side reads no single
table.

The membership recogniser (`exec/ir/build/membership/lift.rs`) lifts a conjunct of a
`Filter` or `PermissionFilter` predicate of the form `x IN (SELECT VALUE k
FROM t WHERE c)` — and its `NOT IN`, `CONTAINS` and `CONTAINSNOT` spellings —
into a `Semi` or `Anti` join on `x = k`, when the subquery's correlation is a
`$parent.path` read and every expression is built from forms the pass can
re-root: a conjunct `r = $parent.p` of `c` is a further key pair, another
correlated conjunct is the join's residual, and an uncorrelated conjunct
stays a filter on the right side, evaluated once. Evaluated once is the
condition as well as the consequence: the subquery's `VALUE` expression, the
conjuncts that reach the build side and every scalar of the pipeline beneath
them must be pure, since one relation now answers every outer row where the
per-row form drew a fresh one. A subquery filtered by `rand::bool()` stays
scalar. A correlated conjunct must additionally be unable to raise: the join
evaluates a key for every left row whatever the build side holds, and a
residual only for the pairs the keys matched, where the per-row form
evaluates the whole condition once per right row per outer row — so `k =
<int> $parent.bad` over an empty right side stays scalar rather than
reporting a cast the statement as written never reaches. The other conjuncts of the
predicate keep their order in a filter of the original kind beneath the join,
so the left side's access path is chosen as before. Keys match by SurrealQL's
`=`, as `IN` and `CONTAINS` compare elements: `NONE` matches `NONE` and
`NULL` matches `NULL`. Lifted from a `PermissionFilter`, the join keeps the
permission frame and its `AUTH LIMIT`, and the barrier rule gains its one
extension: no user expression moves below such a join, and inside an
enclosing permission frame it passes every row, as `PermissionFilter` does.
The right side is the subquery's pipeline as the resolver left it — the table
under its own permission filter, layers and already-lifted joins — so a table
the caller may not read directly is read inside the permission exactly as the
per-row subquery read it, and a checked caller's `EXPLAIN` hides the join's
keys, residual, table, alias and right plan as it hides a permission
predicate, and `EXPLAIN ANALYZE` reports no metrics beneath it.

What the pass does not lift stays a scalar subquery evaluated per row, and is
correct, only slow: a subquery with `LIMIT`, `ORDER BY`, `GROUP BY`, `SPLIT`,
`FETCH`, a field list or a non-table source; a correlation through a function
argument, a closure, a method, a graph lookup or a nested plan; a bare
`$parent`, or `$this` anywhere; a `MATCHES` or `KNN` in the predicate or the
subquery's filter, which names an index of the statement it is written in and
is answered by that statement's access path. `FETCH` stays `LogicalOp::Fetch`:
it lands the fetched record in place of the link and leaves an unset link
unset, which no projection over a `Left` join expresses — a projection lands
an explicit `NONE` — and it leaves a link to another table as it is. The
record-link dereference and the graph lookup stay the expression forms of
[Idioms are expressions](#idioms-are-expressions); a scalar `(SELECT … LIMIT
1)` and an array-valued `(SELECT VALUE …)` in value position — each a `Left`
join followed by an `Aggregate` per left row — are not lifted. Each of these
is a join form the rule set of step 4 can add as one rule under the same
guard-rail the pass holds itself to: a shape is lifted only where the rows are
identical on every language test.

Physically (`vm/map.rs`), the left input is planned with the
pipeline's predicate and ordering offered to its access path less every
conjunct or key that reads the alias or the row opaquely: a join adds to or
removes from the left rows without changing them, so a conjunct over the left
row judges the same value on either side. When the one key pair is `(x,
alias.id)` over a table chain — an `IN (SELECT VALUE id FROM t …)` — the
join is a `Plan::Join` with a `JoinLookup`: the table is read by the left
keys, one batched get per left batch, and the chain over it (permission
filter, layers, filter, permission joins) runs over the fetched records, so
what was one point get per row is one read per batch. That path is taken only where nothing in the chain can raise
(`plan_may_raise`): the lookup evaluates the chain for the rows the left keys
name, where the hash join and the scalar form the lift replaced evaluate it
for the whole relation, and an access path may not remove an evaluation the
plan owes ([What a rewrite may do to an
evaluation](#what-a-rewrite-may-do-to-an-evaluation)).
Otherwise the right input is mapped like any plan and the join (`Plan::Join`,
one form for MATCH binding rows and for document rows with expression keys)
builds it once, after the probe has
yielded its first row — so a right side lifted from an `IN` subquery is never
evaluated when no left row reaches the join — and probes the left rows against
it. The residual reads the build row through the alias whatever the probe row
is: a bare value from a `VALUE` projection or a scalar source has no field to
hold the alias and is emitted as it is, so the residual is evaluated against
an object carrying the alias alone, where every other name answers `NONE` as
it does off the bare value. A MATCH's graph expansion is a
candidate implementation of a lookup join over an edge table.

## What a rewrite may do to an evaluation

The rewrites move, merge, drop and lift nodes, and every one of them is judged
against a single boundary: **a rewrite may not remove an evaluation, and may
reorder one.**

Reordering is allowed because a statement's rows do not depend on the order
its conjuncts are judged in, and the physical planner needs the freedom to
put a predicate where an index answers it. The cost is that an error raised
by a moved conjunct can surface on a row an earlier conjunct would have
refused — stated at the `optimise` row above, and the reason the join
pushdown is not gated on `may_raise`.

Removal is not allowed because an evaluation the statement owes is part of
what the statement does. A `DEFINE PARAM` read judges that definition's
`PERMISSIONS` clause where the read happens; a call may write; a cast may
raise; a `COMPUTED` field's body is all three at once. An engine that
answers `[]` where the statement owes a refusal has not optimised the
statement, it has changed it. So a rule that would make an evaluation happen
*fewer* times — not at all, or only for the rows that survive, or only for
the pairs that match — must first establish that the expression is inert.
Three predicates say what inert means, in widening order. The recogniser asks them
of the logical expression (`exec/ir/build/membership/lift.rs`), and the passes ask the
effect and fallibility analyses (`exec/ir/analysis`) of the ops:

| Predicate | What it says of an expression | Who consults it |
|---|---|---|
| `LogicalExpr::read_only` | nothing in it writes | the recogniser's probe side |
| `may_raise` | it holds a cast, arithmetic, an index, a subquery, a call, or a parameter that is not row-scoped, whose read may fetch a `DEFINE PARAM` — so evaluating it can raise | the recogniser's correlated conjuncts and held terms; the passes ask the fallibility analysis |
| `is_pure` | read-only, no index operator, every builtin deterministic, and no record id whose key is generated (`t:rand()`, `t:ulid()`, `t:uuid()` name a fresh record each time the id is built) | the recogniser's right side, which the join reads once for every outer row |

Which builtins count as deterministic is the registry's answer, not the
signature's: `ScalarFunction::is_deterministic` reports whether a second call
on the same arguments answers the same and leaves nothing behind, and three
kinds of builtin owe `false` — one that draws entropy or reads the clock
(`rand::*`, `time::now`, `array::shuffle`, and `crypto::*::generate`, which
salts afresh on every call); one that reaches past the transaction's snapshot,
to a remote endpoint, a bucket or a sequence, whether to read it or to change
it (`http::*`, `file::*` bar the two accessors on the file value itself,
`sequence::nextval`, `sleep`); and one that runs a body the call site cannot
see (`api::*`, `eval::*`). The flag is applied by wrapping in
`NonDeterministic` at the registration, since the declaration macros describe
a signature rather than the rewrites a call admits, and
`volatile_builtins_are_non_deterministic` in `exec/function/registry.rs` holds
the whole set. Answering `false` only forbids rewrites, so an unclear
classification belongs on that side.

Three of the gates read the plan and not just the expression, because what
the plan does around an expression decides how often it is evaluated. A
`Sort` over at most one row is dropped only where no key reads a layer column
set by a non-inert expression, since `prune` takes a column nothing reads and
the sort was the only reader. A literal array source is read through
`batch_fetch_in_place` before the pushed predicate judges the row, since the
fetch is where a record id's `PERMISSIONS FOR select` is judged ([A source
that yields values](#the-physical-planner--execplanner)). And an id-keyed
join is planned as a lookup join, which reads only the records the left keys
name, only where the chain over the right table is inert — the one place the
*physical* planner asks the question, using `plan_may_raise` because the bind
pass's parameter classification is not in hand there.

The dependence-graph IR planned for the layer after this one makes the same
boundary structural rather than predicated: a side-effecting operation takes
and produces an explicit state value, so a node the state edge reaches
cannot become unreachable, and no inertness test is needed. Until then the
predicates above are the boundary, and a new rule is reviewed against them.

## The steps

Each step lands on its own, leaves the language suite at zero failures, and
deletes what it replaces. All four are in place and are described above as
the plan is.

1. **Idioms become expressions.** Add the expression forms above to
   `LogicalExpr`; lower every `sql::Idiom` to them; make `Split`,
   `Aggregate.group` and `LogicalOrder` carry expressions; give `Schema` a
   declared `FieldPath` and the expression-to-path map; retarget index
   analysis, `row_scope`, `projection` and `fields` utilities onto the forms;
   compile each form to its own `Clause`; delete `LogicalIdiom`,
   `LogicalPart` and `planner/idiom.rs`. A graph
   hop stays a `Lookup` form here. This step
   changes representation only: every lowered path renders and evaluates as
   the idiom did. The one behaviour change in this area lands as its own
   commit right after it: inside `[WHERE …]` the element is `$this` and the
   enclosing row is `$parent`, which is the
   language's scoping.
2. **Computed fields and field permissions are projection layers.**
   `LogicalField::Single` carries a frame and a kind; `resolve` emits the
   layered projections in the runtime's order, `SetField` and `Broadcast`
   expressing nested and wildcard guards; `optimise` moves a predicate
   through a user projection and never through a layer; the physical planner
   offers a predicate to index selection through any chain of
   `PermissionFilter` and layers over a source, withholding what reads a
   layer column, and maps a layer to `Plan::Layer`. Every statement
   shape resolves: `SELECT *`, `$this`, grouped, split, ordered, whole-row
   and multi-source reads all see the layered row, and a resolved source has
   no runtime reducer. A declared kind on a computed field is its column's
   kind.
3. **Join.** `LogicalOp::Join` and its schema; the decorrelation pass,
   lifting the membership subqueries — `IN`, `NOT IN`, `CONTAINS`,
   `CONTAINSNOT` over a `SELECT VALUE` from a table — out of user and
   permission predicates into `Semi` and `Anti` joins in the predicate's
   frame; the join generalised to document rows and a lookup form added;
   the scalar subquery kept as the per-row form of every shape the pass does
   not lift. `FETCH` stays its own operator, and the record-link dereference
   and the graph lookup stay expression forms: each is lifted only once a
   rule can show the rows identical on every language test.
4. **The rewrites.** The IR's passes and the binder's membership recogniser —
   filter pushdown into subqueries and beneath a layer chain, layer-column
   pruning, identity elimination, decorrelation, user-defined function
   inlining, kind-directed simplification — each with golden plan tests for
   where it applies and where it does not, and `plan_source` receives its
   offer from the shape they leave: the filter the layer push places inside a
   chain is the chain's scan predicate. A `DEFINE FUNCTION` body is compiled once into a node-level,
   content-addressed function cache beside the catalog cache and planned
   once per call site of the statement's plan, which is what inlining reads
   and what a call the inlining pass did not inline evaluates. Nothing moves beneath a
   `PermissionFilter`, so an access path declined beneath one still judges
   the predicate in full above the scan; the join forms step 3 left per row
   — `FETCH`, the record-link dereference, the graph lookup, a scalar
   subquery in value position — are candidates for further rules under the
   same guard-rail.

Still open, and small: a source planned in `EnforcementMode::Runtime` compiles
the table's `PERMISSIONS FOR select` from the `&Permission` it is handed
rather than reading the body `TableDefinition::permissions_body` already
carries;
changing that changes the `&Permission` signature at every scan call site.

## Verification

```bash
cd language-tests && cargo run --locked -- run
```

Baseline: 0 failures. Warnings are the work-in-progress tests and
load-dependent timeouts, and vary with machine load.

```bash
cargo test --locked -p surrealdb-core --lib --features kv-mem,gql
cargo test --locked -p surrealdb-expr --lib
cargo test --locked -p surrealdb-catalog --lib
cargo test --locked -p surrealdb-gql
cargo test --locked -p surrealdb-datastore
cargo clippy --locked -p surrealdb-core --all-targets --features kv-mem,gql,scripting,http,jwks,ml,surrealism -- -D warnings
cargo clippy --locked -p surrealdb-core --lib --tests --no-default-features --features kv-mem -- -D warnings
cargo clippy --locked -p surrealdb-expr --all-targets -- -D warnings
cargo clippy --locked -p surrealdb-catalog --all-targets -- -D warnings
cargo clippy --locked -p surrealdb-gql --all-targets -- -D warnings
cargo clippy --locked -p surrealdb-datastore --all-targets -- -D warnings
cargo clippy --locked -p surrealdb-core --lib --features kv-mem,gql -- -D warnings -W clippy::assigning_clones
```

The second clippy line is the shape CI builds core in — one storage engine,
no default features — and catches an item only the GQL or scripting feature
constructs. The last enables the lint the wasm CI pass adds.
