# GQL to MatchPlan lowering (v2)

This is the normative design for `surrealdb/core/src/gql/lower/`. The
lowering turns a parsed GQL AST (`gql::ast`) into a
[`MatchPlan`](../../surrealdb/core/src/expr/match_plan.rs) — the
language-neutral, binding-table IR — carried as a single `LogicalOp::Match`,
which the executor holds as a `TopLevelStatement::Plan`. **No SurrealQL
surface AST is generated**: the execution
planner (`exec/planner/match_plan.rs`) compiles the `MatchPlan`
directly into physical operators (`exec/operators/` — `graph/`, `join/`, and the
generic `bind.rs`/`distinct.rs`). The `MatchPlan` runs only through that
planner; no per-record evaluation exists for it.

`V2_DESIGN.md` is the **authoritative spec** for the IR, the operators, the
planner, and the plumbing. This document is narrower: it specifies what the
*lowering* is responsible for — the semantic analysis, the binding registry, the
conjunct dependency sets, the 3VL guard insertion, and the output spec — and
defers everything about *how the plan executes* to `V2_DESIGN.md`. Where this
document and `V2_DESIGN.md` overlap (the IR shape, the pinned rules R1–R8),
`V2_DESIGN.md` wins; this file does not restate it.

Two corpora pin the behaviour this design relies on:

- **Engine substrate** — `language-tests/tests/gql/lowering_substrate.surql`
  (the E1–E8 pins the §4 guard rules cite, kept for continuity) and
  `exec_substrate.surql` (the v2 operator-level pins: the 3VL guard trio, the
  NULL/NONE sort position R7 relies on, RecordId auto-fetch) pin the
  SurrealQL/engine facts the guard rules and binding-row model depend on.
- **Operator substrate** — the unit tests in `surrealdb/core/src/exec/operators/`
  (`graph/{expand.rs, endpoint.rs, path_expand.rs, distinct_edges.rs}`,
  `join/hash_join.rs`, `bind.rs`, `distinct.rs`, `scan/fetch.rs`, with shared
  fixtures in `test_util.rs`) pin the operator
  semantics the lowering targets (R2–R6, the join/null rules, the FieldState
  fetch boundary).

If either corpus changes in a way that contradicts this design, the design must
be revisited.

## 1. Model mapping

| GQL | SurrealDB |
|---|---|
| node label `(:person)` | table `person` |
| edge type `[:knows]` | RELATE edge table `knows` (records with `in`/`out` RecordIds) |
| property | record field |
| node/edge identity | the record's `id` (binding rows hold full objects, so edges keep `in`/`out`) |
| parameter `$x` | SurrealQL param `$x` (arrives via the `vars` argument) |

## 2. The binding model (lowering's view)

GQL returns **one row per binding** of the pattern variables. v2 makes this
literal: a binding row is a `Value::Object` keyed by binding name, and every
expression the lowering emits is **binding-row scoped** — a variable `v` lowers
to `Idiom[Field("v")]` and `v.x` to `Idiom[Field("v"), Field("x")]`, in every
position (predicate, projection, sort key). There is no `$parent`/`$this`/`__m`
addressing and no scope table: the v1 `Role × ScopeKind` machinery is gone (the
scope-collapse note in `lower/expr.rs`). `V2_DESIGN.md` §3 specifies what each
binding-kind's value *is* at runtime (full record object, edge id, edge-group
array, alternating path array, or `Value::Null` on an optional miss); the
lowering only needs to know that group and path bindings hold composite values
with no addressable field structure, so property access on them is rejected.

The lowering does **not** decide joins, anchors, conjunct placement, or operator
selection — those are the planner's (`V2_DESIGN.md` §1, §6). The lowering
*declares* the structure the planner consumes: which bindings exist and of what
kind, which clause/pattern each element belongs to, which bindings each
predicate reads (its `deps`), and the final output spec.

## 3. The lowering's responsibilities

The lowering is the **semantic analysis** front of the IR contract
(`V2_DESIGN.md` §1: "Lowering owns semantics"). It runs over a `reblessive`
stack because the parser builds arbitrarily deep linear expression chains. Its
responsibilities, by module:

### 3.1 Binding registry — `lower/binding.rs`

A single pass over the whole `MatchItem` tree (plain `MATCH` clauses and the
`OPTIONAL` operands that nest them) in textual order, building the binding
registry that becomes `MatchPlan::bindings`:

- **Declaration vs reuse.** Each user-named pattern variable is declared once,
  at first occurrence, with a `BindingKind` (`Node` / `Edge` / `EdgeGroup` under
  a quantifier / `Path`). Anonymous elements get a hidden `__v<n>` (node) or
  `__e<n>` (edge) binding so every element is addressable and DIFFERENT-EDGES can
  read anonymous edge ids.
- **Node-variable reuse is the join key.** A node variable reused *across*
  patterns/clauses resolves to the **same** `BindingId` — the shared binding the
  planner equi-joins on (R1). The lowering declares the shared id; it never
  builds the join.
- **Within-pattern node repeat is a self-loop.** A node variable reused *within
  one pattern* (`(a)-[…]->(a)`) cannot share the id — there is no join in a
  single chain to materialise the equality, and a shared id would let the second
  occurrence overwrite the first on the binding row. It is rewritten to a fresh
  hidden node binding plus an `id`-equality conjunct (recorded as
  `node_equalities` and emitted by `pattern.rs`), per the `V2_DESIGN.md` §2 IR
  invariant.
- **`optional_depth`.** Each binding records the `OPTIONAL` nesting depth it was
  first declared at (`0` = mandatory). This is the single fact the 3VL guard
  amendment (§4) consults.
- **Anchorability** (R2/`V2_DESIGN.md` §0) is validated per pattern, against the
  bindings present *before* the pattern: every pattern needs ≥1 labelled element
  or ≥1 variable already bound earlier. A pattern that is anchorable in the
  abstract but lies outside the shapes the planner physically realises is also
  rejected here (the `pattern_is_realisable` check mirrors the planner's anchor
  selection exactly, so a cleanly-lowered plan never hits a planner internal
  error — `V2_DESIGN.md` §6 contract).

### 3.2 Clauses, patterns, conjuncts — `lower/pattern.rs`

Per flattened clause: build a `PatternPlan` for each comma-separated pattern
(node/edge steps with the full multi-hop chain intact, edge directions and
quantifiers validated per R6, labels resolved to `TableName`), then collect and
lower the clause's predicates into a flat list of `MatchPredicate`s:

- **Predicate sources, in contract order:** the explicit clause `WHERE`, then the
  inline element `WHERE`s of every pattern, then the property-map equalities
  (`{city: 'London'}` ≡ `<element>.city = 'London'`), element by element.
- **NNF conjunct split.** Each source is split into top-level conjuncts, pushing
  `NOT` through `AND`/`OR` (De Morgan) so conjuncts hidden under negation are
  classified independently; ORs are never distributed. The user's boolean spine
  is otherwise preserved.
- **Dependency sets.** Each conjunct records `deps`: the exact, sorted, deduped
  set of `BindingId`s its variable references resolve to. The planner places each
  conjunct at the earliest stage of its owning clause where `deps ⊆ bound`
  (`V2_DESIGN.md` §6); the lowering only computes the set.
- **Clause ownership (R3).** A conjunct lives on the clause whose pattern scope
  introduces its bindings. For an `OPTIONAL` clause this is structural: a
  predicate written inside the optional attaches to that clause and so compiles
  inside the optional's own subplan (pre-null), while a later clause's predicate
  that merely references an optional binding is owned by that later clause
  (post-null). This is the placement that makes R3's pre-/post-null distinction
  fall out of the plan shape rather than needing a runtime flag.
- **Quantified-edge predicate rule (R6).** A conjunct referencing a quantified
  edge group together with any other binding is rejected — the per-path traversal
  has no place to evaluate a cross-variable constraint.

### 3.3 Expression lowering & guards — `lower/expr.rs`

Lowers each GQL expression with uniform binding addressing (§2) and the
three-valued-logic guards (§4). Value position (projections, sort keys,
comparison operands) lowers two-valued; predicate position (`WHERE`) lowers
three-valued with the guards. Expressions are built directly as the logical
plan's `LogicalExpr`, the scalar type of `MatchPlan`; the planner
compiles them as it compiles every other scalar.

### 3.4 Output spec — `lower/mod.rs`

Builds `MatchOutput` per R7/R8 (§5): the projected columns (with final names,
duplicates rejected, `RETURN *` pre-expanded), the `DISTINCT` flag, the resolved
`ORDER BY` keys, and the `SKIP`/`LIMIT` counts. Also enforces the top-level
structural rejections: an empty query (no `MATCH`), and a query that **leads**
with `OPTIONAL` (R3 — an `OPTIONAL` is a left-outer join and needs a preceding
mandatory clause to join against).

## 4. Three-valued logic (the guard rules)

*(Kept from the v1 contract — verified against `lower/expr.rs` — with the
optional-nullability amendment of `V2_DESIGN.md` §8.)*

SurrealQL comparisons are two-valued over a total order (`NONE`/`NULL` sort below
numbers; `NULL = NULL` is true — E8c), while GQL comparisons with null are
UNKNOWN and `WHERE` keeps only TRUE. After NNF (the operator is complemented
first when a `NOT` was pushed in, so the guard applies to the *effective*
comparison), each leaf lowers as follows. The guard for a set of nullable atoms
is, per atom, the conjunct pair `atom != NONE AND atom != NULL` (built by
`guard_conjuncts`, in that order), AND-ed in front of the comparison.

- **Ordering comparison** `x OP y` (`<` `<=` `>` `>=`): guard **every** nullable
  operand — `x != NONE AND x != NULL AND y != NONE AND y != NULL AND (x OP y)`,
  guarding only operands that can be null/missing (property accesses, params,
  optional-bound bare variables; never literals). Pinned by E8a/E8b.
- **Equality** `=`: guard only when **both** sides are nullable (this covers the
  `NULL = NULL → true` delta — E8c); `x = <literal>` needs no guard.
- **Inequality** `<>`: guard when **either** side is nullable — `NULL != 'A'` is
  true in SurrealQL but UNKNOWN (excluded) in GQL, so a one-sided null must
  exclude the row.
- Guards apply **in every position** (a binding-row predicate has no scope
  distinction in v2; the v1 "applies in lookup conds too" hazard is subsumed).
- **Bare nullable boolean / fallthrough predicate** `b.flag` → `b.flag = true`;
  `NOT b.flag` → `b.flag = false` (UNKNOWN→excluded, FALSE→kept — matches GQL in
  `WHERE` position). Implemented as an equality against an explicit boolean for
  any predicate-position expression that is not a recognised comparison/test.
- `x IS NULL` → `(x = NULL OR x = NONE)`; `IS NOT NULL` → `(x != NULL AND x !=
  NONE)`. GQL cannot observe SurrealDB's NONE-vs-NULL distinction (document in
  user docs).
- `x IS TRUE|FALSE [NOT]` → equality against `true`/`false`; `x IS UNKNOWN` →
  the same null test as `IS NULL`; `IS NOT …` negates the whole test.
- `XOR`: **rejected** (no exactly-equivalent three-valued lowering) —
  *"`XOR` is not supported yet"*.

### 4.1 The optional-nullability amendment (R3)

The one v2 change to the guard machinery. A **bare** variable reference `v` is
nullable iff `v` is optional-bound — declared inside an `OPTIONAL` operand
(`optional_depth(v) > 0`). On an optional miss its whole binding is
`Value::Null` (R3), so a comparison reading it must exclude the pre-null row,
exactly as a property access or param would. A **mandatory** bare variable is
never nullable (its binding is always a full record). Concretely, `nullable()`
and `nullable_atoms()` in `lower/expr.rs` treat `Variable(v)` as a guard atom
exactly when `Scope::variable_is_optional(v)` is true. Property accesses and
params remain unconditionally nullable as in v1.

## 5. Naming, RETURN, DISTINCT, ORDER, SKIP, LIMIT

*(The naming rules are kept from the v1 contract — `lower/naming.rs` is reused
verbatim.)*

- **Column names.** Each projected item gets a final name (the row-object key):
  explicit `AS x` wins; an unaliased item is named by the **verbatim source
  text** of its expression. Duplicate column names are rejected (*"Duplicate
  column name `{name}`"*).
- **`RETURN *`** (R8) expands to all **user-named** bindings — including group
  and path variables — in **alphabetical** order, each column named by the
  variable and carrying the whole binding value. An empty expansion is rejected
  (*"RETURN * requires at least one named pattern variable"*).
- **Reserved names.** GQL variables, aliases and parameters beginning with `__`
  are rejected (the reserved prefix), as are parameters with engine-reserved
  names (`this`, `parent`, `value`, …) — `lower/naming.rs::validate_param_name`.
- **`DISTINCT`** sets `MatchOutput::distinct`; the planner builds the
  `Project → Distinct → Sort → Limit` tail (`V2_DESIGN.md` §6). `ALL` is the
  default no-dedup quantifier.
- **`ORDER BY`** (R7): **without** `DISTINCT`, a sort key is a full binding-row
  expression evaluated pre-projection over **all** bindings — so a key naming a
  RETURN column resolves to that column's *underlying binding-row expression*
  (Sort runs before Project), and any other key lowers directly. **With**
  `DISTINCT`, the key must name a returned column (by dotted name, or by lowering
  to the same expression as one) and the sort references it by name (Sort runs
  post-Project in the DISTINCT tail); any other key is rejected (*"With RETURN
  DISTINCT, ORDER BY may only reference returned columns"*). `NULLS FIRST|LAST`
  is rejected.
- **`SKIP`/`OFFSET`** and **`LIMIT`** lower to `MatchOutput::skip`/`limit` and
  accept an unsigned integer literal or a `$param`.
- **List/record literals** lower to SurrealQL array/object literals in any value
  position.
- **Functions and aggregates** are rejected: the v2 function whitelist is empty
  (*"The function `{}` is not supported yet"*), with a dedicated message for
  aggregates and `count(*)`/`count(DISTINCT …)` forms (*"Aggregate functions are
  not supported yet"*).

## 6. Quantifiers (R6)

A quantified edge binds a **group variable** (`BindingKind::EdgeGroup`, R4); the
chain's far node and the optional path variable are declared as usual. The full
quantifier set is legal — `*` ≡ `{0,}`, `+` ≡ `{1,}`, `?` ≡ `{0,1}`, `{n}`,
`{n,m}`, `{n,}`, `{,m}`, `{,}` — and the lowering normalises each to a
`{min, max: Option<u32>}` `EdgeQuantifier`. The **only** lowering rejection is
`max < min` (*"The quantifier maximum must not be smaller than its minimum"*).
The per-path semantics, the `min == 0` zero-length emission, and the
unbounded-form termination via edge-uniqueness-within-path are all the
`PathExpand` operator's job (`V2_DESIGN.md` §5, pinned by `path_expand.rs`); the
lowering only emits the bounds. An inline predicate on a quantified edge is
edge-only (§3.2). Property access on the group variable is rejected (§2).

> This is the v2 generalisation of the v1 restriction (`min == 1`,
> distinct-reachable). See `REFERENCE.md` §(h) for the cardinality change a user
> upgrading from the v1 draft will observe.

## 7. Rejection ledger

The lowering's rejections, grouped by where they live. Messages are quoted so
they are searchable; the authoritative source is the `bail!`/`syntax_error!`
sites in `lower/{binding,pattern,expr,naming,mod}.rs`.

**Pattern/binding rejections** (`binding.rs`, `pattern.rs`):

- non-anchorable pattern — *"Cannot choose a starting table for this pattern:
  label at least one node or reuse a variable bound by an earlier pattern"*;
- anchorable-but-not-realisable shape — *"This MATCH pattern shape is not
  supported yet"*;
- repeated edge/group variable — *"Edge variable `{}` cannot be repeated"* (R2:
  an edge cannot bind twice, so the join is always empty);
- repeated path variable — *"Path variable `{}` is declared more than once"*;
- kind-mismatched reuse — *"Variable `{}` is already bound as {} but reused as
  {}"*;
- optional-rebind — *"Variable `{}` was first bound inside an OPTIONAL and cannot
  be re-declared outside it"*;
- undirected / multi-directional edges — *"Undirected and multi-directional edge
  patterns are not supported yet"*;
- label expressions — *"Label expressions (`!`, `&`, `|`, `%`) are not supported
  yet"*;
- quantifier `max < min` — *"The quantifier maximum must not be smaller than its
  minimum"*;
- cross-variable quantified-edge predicate — *"A predicate inside a quantified
  edge may only reference that edge"*.

**Expression rejections** (`expr.rs`):

- property access on a group/path variable — *"Property access on a group or path
  variable is not supported yet"*;
- `XOR` — *"`XOR` is not supported yet"*;
- aggregates — *"Aggregate functions are not supported yet"*; any other function
  — *"The function `{}` is not supported yet"*.

**Output / structural rejections** (`mod.rs`):

- empty query — *"A query without a MATCH clause is not supported yet"*;
- leading `OPTIONAL` — *"A query cannot start with OPTIONAL MATCH: OPTIONAL is a
  left-outer join and needs a preceding MATCH to join against"*;
- `NULLS FIRST|LAST` — *"`NULLS FIRST`/`NULLS LAST` ordering is not supported
  yet"*;
- DISTINCT ORDER scope — *"With RETURN DISTINCT, ORDER BY may only reference
  returned columns"*;
- duplicate column — *"Duplicate column name `{}`"*; empty `RETURN *` — *"RETURN
  \* requires at least one named pattern variable"*.

Five of these are **new in v2** (they guard constructs v1 rejected wholesale):
repeated edge variable, kind-mismatched reuse, optional-rebind,
cross-variable quantified-edge predicate, and property-access-on-group/path-var.
The DISTINCT-ORDER message changed from v1's *"ORDER BY may only reference RETURN
items"*. Fourteen former v1 rejections became features — see `REFERENCE.md` §(h)
for the full table with v1's quoted error texts.

## 8. Worked example

`MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 RETURN a.name AS a_name, b.name AS b_name`
lowers to a `MatchPlan` with three bindings (`a:Node`, `k:Edge`, `b:Node`), one
mandatory clause with one pattern (`a -[k]-> b`) and one predicate (deps `[k]`),
and two output columns. The predicate's guard expansion (§4, ordering
comparison, `k.since` nullable) makes the lowered expression
`k.since != NONE AND k.since != NULL AND k.since > 2020`. The deterministic
`MatchPlan` renderer (`expr/match_plan.rs::impl ToSql`) reproduces this query as

```
MATCH (a:person)-[k:knows]->(b:person) WHERE k.since != NONE AND k.since != NULL AND k.since > 2020 RETURN a.name AS a_name, b.name AS b_name
```

— the rendering used by EXPLAIN and the snapshot tests, with predicate slots
delegating to `Expr`'s `ToSql` so the guard shapes stay diffable. The
corresponding *plan tree* (anchor `Bind`, `Expand` with the predicate pushed
onto it, `Project`) is pinned in `V2_DESIGN.md` §6 (i). Further worked plan trees
for joins, OPTIONAL and quantified paths are in `V2_DESIGN.md` §6 (ii)–(iv).

## 9. Result shape caveat (document, don't fight)

Binding rows are `Value::Object` keyed by column name; SurrealDB objects are
key-ordered, so the `RETURN`-list column order is not preserved in the value
itself. The GQL AST keeps the ordered column list (`ReturnClause.items`) and the
`MatchOutput::columns` order is preserved in the IR for a future row-set wire
format, but a client reading the result object must not rely on column order.
