# GQL v2 design contract — MatchPlan, binding-table execution

Implementation contract for the v2 program (plan: 4 PRs — foundation/cutover, joins,
OPTIONAL, polish). Read together with `REFERENCE.md` (grammar) and `LOWERING.md`
(v1 contract; rewritten in PR-D). Line refs are to main @ `6b814d436`.

## 0. Pinned semantic rules (normative)

- **R1** Comma patterns ≡ sequential MATCH for equi-joins on shared variables; they
  differ only in edge-uniqueness scope (R2 is per MATCH statement).
- **R2** Default match mode = **DIFFERENT EDGES**: within one MATCH statement (incl.
  quantifier expansions) no edge record binds twice; nodes repeat freely. (Pinned vs
  implementation consensus — Kusto/Spanner GQL; confirm ISO 39075 §16.4 wording when
  accessible; the pin stands either way.)
- **R3** OPTIONAL = left-outer vs the accumulated binding table. Unmatched ⇒ every
  binding first introduced inside binds `Value::Null` (incl. group/path — Null, not
  `[]`). Inside-optional predicates evaluate pre-null (part of the optional's own
  match); outside predicates post-null. Chained OPTIONALs left-to-right. Block forms
  are all-or-nothing units.
- **R4** Edge variable under a quantifier = **group variable**: ordered LIST of the
  traversed edge records; one row per path; `[]` for a zero-length path; Null on
  optional miss.
- **R5** Path value (`RETURN p`) = alternating array `[node, edge, node, …, node]`
  of full records (2k+1 elements; single-node path = `[node]`).
- **R6** Quantifiers `* + ? {n} {n,m} {n,} {,m} {,}`: one row per path at every
  depth in `[min, max]`; unbounded forms terminate via edge-uniqueness-within-path
  (subsumed by R2); `min == 0` emits the zero-length path (target = source, empty
  group, `[node]` path).
- **R7** ORDER BY: without DISTINCT — full expressions over all bindings, evaluated
  pre-projection on binding rows. With DISTINCT — returned columns only; error:
  "With RETURN DISTINCT, ORDER BY may only reference returned columns".
- **R8** `RETURN *` = all user-named bindings (incl. group/path vars), alphabetical.
- **R9** Path search (`pathPatternPrefix`, added after the v2 program): the prefix
  `ALL | ANY [k] | ALL SHORTEST | ANY SHORTEST | SHORTEST k | SHORTEST [k] GROUP(S)`
  on a single variable-length segment selects, per endpoint pair `(start, end)`,
  which paths survive; shortest is by hop count (unweighted). It routes to
  `PathExpand` (every path / `ANY [k]` per-terminal cap) or the BFS
  `ShortestPathExpand` (the SHORTEST family). Path modes: because R2 (DIFFERENT
  EDGES) already forbids an edge repeating within a path, `WALK` (the ISO default)
  and `TRAIL` both reduce to the existing edge-unique traversal; only `SIMPLE`
  (no repeated node bar a close onto the start) and `ACYCLIC` (no repeated node)
  add a constraint. A selective search (`ANY`/`*SHORTEST`) bound on its far node
  (a reverse anchor) traverses the segment backwards; per-endpoint grouping and
  path length are symmetric so selection is unaffected, and the operator flips
  the emitted path/group back to the pattern's written order (`reversed`). A
  self-loop (`start == end` binding) and a prefix on a non-single-segment
  pattern are rejected.
- **Joins & null**: a Null binding never equi-joins (excluded from hash build; for
  Inner also from probe); Left passes null-keyed probe rows through null-filled.
- **Anchorability**: every pattern needs ≥1 labeled element OR ≥1 variable already
  bound by an earlier pattern/clause; else lowering rejection: "Cannot choose a
  starting table for this pattern: label at least one node or reuse a variable
  bound by an earlier pattern". No whole-graph scans.
- **Optional-miss value is `Value::Null`** (not NONE): `b IS NULL` lowers to
  `(b = NULL OR b = NONE)` → TRUE ✓; `b.x` on Null yields NONE → ordering guards
  exclude ✓; bare `RETURN b` surfaces NULL.

## 1. Division of labor (the IR contract)

**Lowering owns semantics**: binding registry (kinds Node/Edge/EdgeGroup/Path;
hidden `__e<n>`/`__v<n>`; `user_named`), variable resolution (node-var reuse = join
key; edge/group/path reuse and kind-mismatched reuse = rejection), NNF conjunct
splitting with exact `deps`, 3VL guard insertion (v1 machinery verbatim + ONE
amendment: bare `Variable(v)` is nullable iff `optional_depth(v) > 0`), column
naming (naming.rs verbatim), ORDER alias resolution, anchorability validation,
RETURN * expansion. **Planner owns physical choices**: anchor selection, chain
construction, conjunct placement (earliest stage where `deps ⊆ bound`; prefix-strip
rewrite for scan pushdown), intra/inter-clause joins, DistinctEdges insertion,
tail assembly. Neither re-derives the other's work.

## 2. The IR — new `surrealdb/core/src/expr/match_plan.rs`

```rust
pub(crate) type BindingId = u32;

/// Invariants the lowering guarantees (planner relies on, never re-derives):
/// - every Expr is BINDING-ROW scoped (`a.x` → Idiom[Field("a"),Field("x")]),
///   3VL guards already inserted;
/// - MatchPredicate::deps is the exact set of bindings the expr reads;
/// - conjuncts are NNF-split and live on the clause whose pattern scope owns them
///   (critical for OPTIONAL);
/// - column names are final (naming rules applied; duplicates rejected);
/// - ORDER BY aliases resolved (non-DISTINCT → source exprs; DISTINCT → columns);
/// - repeated pattern variables rewritten to hidden bindings + equality conjuncts;
///   anonymous edges needing DIFFERENT-EDGES tracking have hidden bindings;
/// - every pattern is anchorable (rule in §0).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchPlan {
    pub(crate) bindings: Vec<BindingDef>,        // index == BindingId
    pub(crate) clauses: Vec<MatchClausePlan>,    // textual order; >= 1
    pub(crate) output: MatchOutput,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BindingDef { pub name: String, pub kind: BindingKind, pub user_named: bool }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BindingKind { Node, Edge, EdgeGroup, Path }

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchClausePlan {
    // The all-or-nothing OPTIONAL block this clause belongs to (R3), or None for a
    // mandatory clause. SINGLE source of truth for "is this clause optional":
    // `is_optional()` == `optional_group.is_some()` (no separate bool to drift).
    // Clauses sharing an id are one left-outer unit; distinct ids chain L-to-R.
    pub(crate) optional_group: Option<u32>,
    pub(crate) patterns: Vec<PatternPlan>,
    pub(crate) predicates: Vec<MatchPredicate>,  // clause-owned NNF conjuncts
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PatternPlan {
    pub(crate) path_var: Option<BindingId>,      // kind == Path
    pub(crate) start: NodeStep,
    pub(crate) steps: Vec<(EdgeStep, NodeStep)>, // multi-hop chains
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NodeStep { pub binding: BindingId, pub label: Option<TableName> }
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EdgeStep {
    pub(crate) binding: BindingId,               // Edge, or EdgeGroup when quantified
    pub(crate) label: Option<TableName>,
    pub(crate) direction: ExpandDirection,       // Out | In
    pub(crate) quantifier: Option<EdgeQuantifier>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ExpandDirection { Out, In }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EdgeQuantifier { pub min: u32, pub max: Option<u32> }

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchPredicate { pub expr: Expr, pub deps: Vec<BindingId> } // deps sorted+deduped
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchOutput {
    pub(crate) columns: Vec<MatchColumn>,        // explicit; RETURN * pre-expanded
    pub(crate) distinct: bool,
    pub(crate) order: Vec<MatchOrder>,
    pub(crate) skip: Option<Expr>, pub limit: Option<Expr>,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchColumn { pub name: String, pub expr: Expr }
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MatchOrder { pub expr: Expr, pub ascending: bool }
```

### Variant plumbing (`Expr::Match(Box<MatchPlan>)`)

- `expr/expression.rs`: variant; `read_only() → true`; `is_static() → false`;
  `needs_parentheses() → true`; `compute()` arm → hard error "GQL MATCH requires
  the streaming execution engine; it cannot run under the compute-only planner
  strategy" (precedent: the `Expr::Explain` compute arm). **`impl ToSql for Expr`
  must special-case Match BEFORE the sql conversion** (it round-trips via
  `From<expr::Expr> for sql::Expr`), delegating to a dedicated deterministic
  `impl ToSql for MatchPlan` (GQL-ish rendering; never panics).
- `sql/expression.rs` `From<expr::Expr> for sql::Expr`: Match arm →
  `sql::Expr::Literal(Literal::None)` + `tracing::error!` + `debug_assert!(false)`,
  with the documented invariant: **`Expr::Match` is only constructed by the GQL
  lowering at top level and never enters a `sql::Ast`, the catalog, or `Revisioned`
  serialization** (which serializes Expr as SurrealQL text). `From<sql::Expr>`
  needs no arm.
- `expr/visit.rs`: Visit + MutVisitor arms walking every reachable Expr
  (predicates, columns, order, skip, limit).

## 3. Binding rows

Row = `Value::Object` keyed by binding name.

| Kind | Value | Source (security-relevant) |
|---|---|---|
| Node | full record object | TableScan pipeline (anchors) / FieldState-aware fetch (targets) |
| Edge (user-named) | full edge record | FieldState-aware fetch |
| Edge (hidden) | `Value::RecordId` (id only) | adjacency-key decode, no fetch |
| EdgeGroup | `Value::Array` of edge objects, path order | PathExpand |
| Path | alternating array per R5 | PathExpand (already-fetched objects) |
| optional miss | `Value::Null` per introduced binding | OptionalExpand/LeftJoin null template |

Full objects, not ids: Sort's FieldPath extraction doesn't auto-fetch; per-field
auto-fetch costs; joins/DistinctEdges read `obj.id`. One-level-deeper RecordId
links still auto-fetch through the existing permission-aware `fetch_record` path.

## 4. W0 — FieldState-aware fetch helper (CRITICAL, lands first)

`exec/operators/scan/fetch.rs` (+ small `scan/common.rs` extension):
`resolve_record_batch`'s table-permission logic (scan/common.rs:~130-212) PLUS the
`build_field_state` / `compute_fields_for_value` / `filter_fields_by_permission`
machinery (scan/pipeline.rs:~761-987), FieldState cached per table for the
operator's lifetime. Every binding fetch (Expand / EndpointBind / PathExpand) goes
through it: **binding contents ≡ what a SELECT on that table would return.** Do NOT
use bare `resolve_record_batch` (it skips field-level permissions — the possible
pre-existing `GraphEdgeScan FullEdge` gap is filed separately; v2 must not inherit
it). SECURITY_GUIDE.md gains this invariant in PR-D.

## 5. Operators — `surrealdb/core/src/exec/operators/`

The binding-table operators are grouped by reusability: graph-specific traversal under
`graph/` (`expand.rs`, `path_expand.rs`, `endpoint.rs`, `distinct_edges.rs`), the
language-neutral relational join under `join/hash_join.rs`, the generic `bind.rs` /
`distinct.rs` at the operators root, and the FieldState-aware fetch helper under
`scan/fetch.rs`. (The `graph/` and `join/` split is what makes the join reusable by a future
relational frontend — see §10.)

Conventions for all: `metrics: Arc<OperatorMetrics>` + `monitor_stream`;
`buffer_stream(input.execute(ctx)?, input.access_mode(), input.cardinality_hint(),
ctx.root().ctx.config.exec.operator_buffer_size)`; `access_mode` combines children +
embedded predicate exprs; `required_context` ≥ Database; WASM cfg patterns as in
`scan/graph.rs`; EXPLAIN-stable `name()`/`attrs()` (predicates via PhysicalExpr
ToSql). First: extract `compute_graph_ranges`/`decode_graph_edge`/cursor batching
from `scan/graph.rs` into `pub(crate)` `scan/graph_keys.rs` (~130 lines moved).

- **Bind** (~120L): `{ input, name }` — wraps record values into `{name: value}`
  rows. Anchor = ordinary `TableScan` via existing source machinery (permissions /
  computed fields / pre-decode filter for free); pushed single-binding conjuncts
  land in `TableScan.predicate` after prefix-strip. Delegates cardinality/ordering.
- **Expand** (~450L): `{ input, source, direction, edge_tables, edge_binding:
  Full(name)|IdOnly(name), target_binding, target_label, predicate:
  Option<Arc<dyn PhysicalExpr>>, optional: bool }`. Per row: source id (Null source
  → null-row if optional else skip) → adjacency via graph_keys → batch FieldState
  fetch of edges → target rid from edge `out`/`in` per direction → label filter →
  batch FieldState fetch of targets (miss/deny drops candidate) → candidate row =
  input ∪ {edge, target} → `predicate` eval (EvalContext::with_value, as Filter
  does) → emit. `optional: true` (EXPLAIN name "OptionalExpand"): zero survivors →
  one row with Null for edge+target. Do NOT wrap GraphEdgeScan (it flattens away
  input-row correlation). Anonymous-edge embedded-target fast path = later perf PR.
- **EndpointBind** (~200L): `{ input, edge, field: In|Out, target_binding,
  target_label }` — bind a node from a bound edge's endpoint; FieldState fetch;
  1:≤1, order-preserving.
- **HashJoin** (~450L, PR-B): `{ build, probe, keys: Vec<String>, join_type:
  Inner|Left|Cross, null_template: Vec<String>, residual: Option<Arc<dyn
  PhysicalExpr>> }`. Key = `Vec<Value>` of `<binding>.id` per side (FieldPath
  extraction, no fetch). Build = GroupMap pattern (hash bucket + linear probe over
  PartialEq, per Aggregate aggregate.rs:~220-299) plus an insertion-ordered
  `Vec<Value>` of build rows so `Cross` emits deterministically (HashMap iteration
  order is process-random). Null/None in any key ⇒ excluded from build (Inner:
  also probe; Left: pass through null-filled). The optional `residual` (SQL `ON`)
  is evaluated against each merged (build∪probe) row and participates in the match
  decision — for `Left` a probe row with no residual-passing build row is
  null-filled (NOT dropped); this is how a correlated `OPTIONAL` predicate
  spanning the optional body and the accumulator gates the left-outer match
  (mirrors `NestedLoopJoin.cond` in surrealdb/surrealdb#7024). Probe streams; emit
  probe∪build per (residual-passing) match; Left miss → probe + null_template
  Nulls. Guards: `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (default 1_000_000) bounds the
  in-memory build set (spill = future, Aggregate stance);
  `SURREAL_GQL_MAX_OUTPUT_ROWS` (default 1_000_000) bounds the cumulative emitted
  rows (the Cross product / high-fan-out output, which can dwarf either side). The
  build-drain and probe loops poll `ctx.cancellation()` so a runaway join is
  interruptible. `children() = [build, probe]`.
- **PathExpand** (~500L): `{ input, source, direction, edge_tables, min,
  max: Option<u32>, target_binding, target_label, group_binding, path_binding }`.
  Per row: DFS over partial paths `{tip, edges: Vec<{id, obj}>, nodes}`;
  zero-length emission when `min == 0` (label-checked source; `[]` group; `[node]`
  path); extension = adjacency of tip → **edge-uniqueness within path**
  (`edges.contains(id)` — termination for unbounded) → FieldState fetch edge +
  next node (deny/miss prunes branch); emit at every depth in `[min, max]` whose
  node passes `target_label`: input ∪ {target, group: Array(edges), path:
  alternating array}; continue past emission while `d < max`. Guard:
  **per-source-row** live+emitted counter vs `SURREAL_GQL_MAX_PATH_ROWS` (default
  1_000_000) → ControlFlow::Err naming the knob (resets per input row — see §9).
  Intermediate nodes are always fetched (permission-prune semantics require it).
- **DistinctEdges** (~150L, PR-B): per-MATCH-statement R2 enforcement — collect ids
  from the clause's edge-ish bindings (Object→`.id`, RecordId→self, Array→each
  element id, Null→skip); row survives iff pairwise distinct. Planner skips it when
  edge-table sets are statically disjoint. (Intra-group distinctness is inside
  PathExpand.)
- **Distinct** (~150L): whole-row dedup above Project (GroupMap-style seen set;
  first occurrence, order preserved). DISTINCT pipeline: Project → Distinct →
  Sort(columns) → Limit. Seen-set rides the join knob.
- **Reused unchanged**: Filter, Sort (pre-Project over binding rows ⇒ R7), Limit,
  Project (explicit columns ⇒ hidden bindings vanish), Timeout slot reserved.

## 6. Planner — `exec/planner/match_plan.rs` (+ dispatch arm in planner.rs)

`plan_match` NEVER returns PlannerUnsupported/Unimplemented (internal failures are
real errors; no compute fallback exists).

Fold clauses over an accumulator `(plan, bound_set)`:
- **plan_clause** (non-optional): per pattern → chain: anchor = labeled node
  (TableScan+Bind; prefix-strip pushdown) | labeled edge (TableScan+Bind+
  EndpointBind×2, prior steps reversed) | already-bound shared variable (expand
  from accumulated rows); steps → Expand (no quantifier) or PathExpand
  (quantifier). Conjunct placement: each MatchPredicate at the earliest stage of
  the owning clause where `deps ⊆ bound` — TableScan binder ⇒ prefix-strip into
  scan predicate (bail to Filter on whole-record refs); Expand binder ⇒
  `Expand.predicate`; else Filter above the binder; cross-pattern deps ⇒ Filter
  above the intra-clause join. Patterns combine via HashJoin Inner (shared ids) /
  Cross. Wrap with DistinctEdges when ≥2 edge-ish bindings (post the disjoint-label
  skip).
- **Clause combine**: non-optional → HashJoin Inner/Cross with accumulator.
  Optional: single-hop-from-bound-source ⇒ `Expand{optional:true}` fast path; else
  LeftJoin(accumulator=probe, clause subplan=build) on shared ids with
  null_template = clause-introduced bindings — inside-predicates compile inside the
  subplan (R3 structurally).
- **Tail**: non-DISTINCT ⇒ Sort(order exprs over bindings) → Limit(skip, limit) →
  Project(columns). DISTINCT ⇒ Project → Distinct → Sort(columns) → Limit.
- Prefix-strip rewrite util (~60L): clone Expr, rewrite `Idiom[Field(b), rest…]` →
  `Idiom[rest…]`; bail if any bare `Field(b)` whole-record ref remains.

### Worked plan trees (pin as the first EXPLAIN snapshots)

(i) `MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2020 RETURN a.name, b.name`
```
Project [columns: a.name, b.name]
    Expand [dir: ->, tables: knows, edge: k, node: b, target_label: person,
            predicate: k.since != NONE AND k.since != NULL AND k.since > 2020]
        Bind [binding: a]
            TableScan [table: person]
```
(ii) `MATCH (a)-[:x]->(b), (c)-[:y]->(b) RETURN a, c` — PR-B
```
Project [columns: a, c]
    HashJoin [type: Inner, keys: b]
        EndpointBind [edge: __e0, field: out, node: b]
            EndpointBind [edge: __e0, field: in, node: a]
                Bind [binding: __e0]  ← TableScan [table: x]
        EndpointBind [edge: __e1, field: out, node: b]
            EndpointBind [edge: __e1, field: in, node: c]
                Bind [binding: __e1]  ← TableScan [table: y]
```
(DistinctEdges elided: x,y statically disjoint.)
(iii) `MATCH (a:person) OPTIONAL MATCH (a)-[k:knows]->(b) RETURN a.name, b.name` — PR-C
```
Project [columns: a.name, b.name]
    OptionalExpand [dir: ->, tables: knows, edge: k, node: b]
        Bind [binding: a]  ← TableScan [table: person]
```
(iv) `MATCH p = (a:person)-[:knows]->{1,3}(b:person) RETURN p, b ORDER BY a.age`
```
Project [columns: p, b]
    Sort [order: a.age ASC]
        PathExpand [source: a, dir: ->, tables: knows, min: 1, max: 3,
                    node: b, target_label: person, path: p]
            Bind [binding: a]  ← TableScan [table: person]
```

## 7. Strategy matrix & plumbing

- `.gql` language-tests default to `[all-ro, best-effort]` (dialect-aware default
  in `language-tests/src/tests/schema/mod.rs` + `cmd/run/mod.rs`); one pinned test
  `errors/compute_only_strategy.gql` (`planner-strategy = ["compute-only"]`)
  asserts the compute-arm error.
- `gql/mod.rs`: `pub struct PreparedGqlQuery(pub(crate) LogicalPlan)`
  (+Debug+ToSql via MatchPlan renderer); `lower` / `parse_to_plan_with_settings`
  (replaces `parse_to_ast_with_settings`) / `parse_with_capabilities` return it;
  experimental gate unchanged.
- `kvs/ds.rs`: `parse_gql` → `PreparedGqlQuery`; `execute_gql` signature
  unchanged; new `pub process_gql(PreparedGqlQuery, sess, vars)` + pub(crate)
  cancel/txn variants (split `process_with_transaction_inner` into ast-shim +
  plan-level inner).
- `rpc/protocol.rs`: `QueryForm::Plan(PreparedGqlQuery)` added; `Parsed(Ast)`
  untouched (12 other call sites); run_query 8→12 arms; gql handler builds Plan.
- language-tests arm: `parse_to_plan_with_settings` + `dbs.process_gql`.
  (Deferred: there is no `[test] explain` harness key. Plan-shape coverage lives
  in Rust snapshot tests over the `MatchPlan` `ToSql` renderer —
  `surrealdb/core/src/gql/lower/test.rs` and the `to_sql_renders_*` tests in
  `surrealdb/core/src/expr/match_plan.rs` — not in the `.gql` corpus. The `.gql`
  corpus cannot bind query params either: the harness passes `vars: None` for
  both the `.surql` and `.gql` arms, so param-dependent constructs
  (`MATCH … LIMIT $n`) must use literals until a `params` seam is added.)
- Fuzz target textually unchanged; dict gains OPTIONAL/brace tokens.

## 8. Frontend (lowering) contract

- AST: `MatchQuery.items: Vec<MatchItem>`; `MatchItem = Match(MatchClause) |
  Optional(OptionalBlock{items, span})`; `MatchClause.optional` dropped. Parser:
  `parse_optional_operand` (3 grammar forms, object-recursion-budgeted) — PR-C.
- New `lower/binding.rs`: registry (BindingInfo{name, kind, declared,
  optional_depth, first_stage}); resolve-or-declare; node-var reuse = join key;
  other reuse/kind-mismatch = reject; naming.rs validations verbatim.
- `lower/pattern.rs` rewritten: conjunct collect/NNF-split/walk_variables kept
  (~190L); emits MatchClausePlan/PatternPlan with `deps` computed; quantifier
  validation per R6 (max<min stays rejected; cross-variable refs inside a
  quantified edge's inline predicate rejected: "a predicate inside a quantified
  edge may only reference that edge").
- `lower/expr.rs`: guards/NNF/literals/params verbatim (~480L; still building
  sql::Expr → `.into()` per slot is acceptable); Role×ScopeKind deleted → uniform
  binding addressing; `nullable()` amendment (Variable nullable iff optional-bound).
- `lower/mod.rs` rewritten: output spec per R7/R8 (column naming + dup-check +
  lower_count reused).
- Rejection ledger: 14 flip to supported; 23 survive; 1 message change (DISTINCT
  ORDER); 5 new (repeated edge var; kind-mismatched reuse; optional rebind;
  cross-var quantified-edge predicate; property access on group/path vars —
  "not supported yet").
- Snapshot strategy: MatchPlan-structure rendering (deterministic compact
  renderer; predicate slots via ToSql so guard shapes stay diffable) replaces the
  v1 ToSql SELECT snapshots; `assert_rejects` harness reused.

## 9. Guards (cnf knobs)

`gql_max_path_rows` (1M), `gql_max_join_build_rows` (1M), `gql_max_output_rows`
(1M) — `ExecConfig` fields in `surrealdb/core/src/exec/config.rs` (NOT global
statics: every operator that reads them already holds the execution config via
`ctx.root().ctx.config.exec`, so they are per-datastore and settable
programmatically as well as via the `SURREAL_GQL_MAX_*` env vars, which
`ConfigMap::from_env` routes into the same fields). Errors name the env knob.
`SURREAL_GQL_MAX_PATH_ROWS` is enforced **per source row** in `PathExpand` (the
live+emitted counter resets for each input row), so it caps the single-source
combinatorial explosion that drives live DFS memory; the aggregate output ceiling
is `N_source_rows × SURREAL_GQL_MAX_PATH_ROWS`, with `N_source_rows` bounded by the
anchor scan's cardinality. `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` bounds the in-memory
`HashJoin` build set (and the `Distinct` seen set, which rides the same budget).
`SURREAL_GQL_MAX_OUTPUT_ROWS` bounds the **cumulative emitted rows** of `HashJoin`
(the `Cross` cartesian product, or a high-fan-out equi-join, can emit far more
rows than either side holds) and single-hop `Expand`'s fan-out — a distinct axis
from the build/seen budgets. Separately from these row ceilings, every operator
that does heavy work without pulling a fresh upstream batch — `HashJoin`
(build-drain + probe), `PathExpand` (per-batch + DFS step + adjacency cursor),
`Expand` (per-batch + adjacency cursor), plus the per-batch loops of `Bind`,
`Distinct`, `DistinctEdges`, `EndpointBind` — polls `ctx.cancellation()` so a
long-running MATCH is interruptible on client disconnect / query timeout (the
streaming buffer/monitor wrappers inject no cancellation, so each operator polls
itself, matching `scan/table.rs`).

## 10. Relational convergence (future Postgres)

The execution operators are the HOW (physical plan) and are deliberately language-neutral:
they carry no `MatchPlan` IR types and operate purely on binding rows (`Value::Object` keyed by
binding name, §3). The `graph/` vs `join/` split exists so the relational operators can be
shared by a future SQL frontend (e.g. Postgres wire-protocol support) without disturbing the
graph-specific traversal operators.

When that frontend lands, the expected shape is:

- A composable **`Expr::Join`** logical node (a binary relational operator whose children are
  sub-`Expr`s) lives in `Expr` *alongside* `Expr::Match` — both are single-statement logical
  units in the common `Expr` layer; convergence happens there, not by collapsing one into the
  other. (`Expr::Match` is a whole `MATCH…RETURN` query block; `Expr::Join` is one operator
  within a SELECT.)
- Both lower to the same `operators/join/` family. Today that family is `HashJoin` only
  (equi-join / cross); the relational planner additionally needs nested-loop (theta joins),
  index-nested-loop, and sort-merge — they belong in `operators/join/` next to `HashJoin`.
- `HashJoin`'s `keys: Vec<String>` (binding names, `<binding>.id` per side) generalizes to a
  composite `Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>` for arbitrary/asymmetric
  equi-join conditions (the only GQL→SQL shape change in the operator). See `operators/join/mod.rs`.
- The binding-row convention and the FieldState-aware `scan/fetch.rs` helper are the shared
  substrate; a relational frontend reuses both unchanged.
