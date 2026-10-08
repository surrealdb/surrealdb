# The logical rules

Every logical rewrite the executor architecture names, with the layer it belongs to under
[ADR 0003](../adr/0003-stateless-logical-plan-cached-physical-plan.md), what gates it, and what
proves it. This is the work queue for the logical layer, and the logical layer is finished — ready
for the physical layer to be built on it — when every row marked **logical** is either implemented
and proven or recorded as not worth doing.

The source list is the "Logical Plan" section of the architecture document in Notion, together
with its Mutations, Subqueries, Blocks and Control flow sections.

---

## What makes a rule logical

A rule is logical when all of these hold. Failing any one makes it physical, whatever it looks like.

1. **It is sound under every catalog.** Not "under the schemas we tested": under every table, field,
   index, permission, event and function definition the statement could be planned against.
2. **It reads no session state.** Not the caller's capabilities, principal or auth, and not a
   parameter's value.
3. **It evaluates nothing whose answer depends on when the statement runs.** A clock read, an
   entropy draw and an external call are all evaluated at execution; see
   [Non-determinism](#non-determinism).
4. **It does not change what `resolve` will find.** `resolve` builds each source's stack —
   permission filter, field guards, computed-field layers — from what that source's own pipeline
   reads, and resolves each input of a `Join` with fresh stages. So, before resolution:
   - Moving a read between the stages of one pipeline is sound: the stack is built beneath every
     user stage, and the read is still counted.
   - **Removing the last read of a field removes an evaluation**: the computed body or guard that
     read would have made `resolve` emit. A field read is therefore not inert with respect to
     removal until the catalog says it is.
   - **Creating a join hides reads**: a key in `Join.on`, a residual, and the stages above the join
     are not counted into either input's stack. Until `resolve` counts reads through a join, a rule
     that creates one is physical.
5. **It keeps the evaluation contract.** A rewrite may not remove an evaluation unless the
   expression is inert, and may not move one across an evaluation that can raise
   (`AST_TO_EXECUTION.md`, "What a rewrite may do to an evaluation"). Before resolution, *inert*
   is narrower than after: a field read (4, above), a `DEFINE PARAM` read (every parameter that is
   not row-scoped), a call to a user function and a record dereference are none of them inert.

### Non-determinism

| effect | example | ruling |
|---|---|---|
| clock | `time::now()` | per call, per row, in every position but a `WHERE`, where it is one value for the statement ([ADR 0006](../adr/0006-clock-reads-keep-their-evaluation.md)). Never baked into a plan: a condition's value is taken when the statement starts |
| entropy | `rand::uuid()`, `t:ulid()` | per call site, per row. Never folded, never collapsed by CSE, never hoisted |
| external | `http::get` | evaluated exactly where and as often as written. Never folded, hoisted, deduplicated, or reordered past a fallible unit — the count is observable to the server |

---

## Proving a logical rule, in isolation

The logical layer is tested without the physical layer and without a datastore. What each rule
owes:

- **It runs on the unresolved plan.** `bind` runs lowering, then the logical rules, then `resolve`,
  then the catalog-dependent rules, then the planner. The logical rules are tested on plans that
  have never been resolved.
- **A non-vacuity test.** A property proves a rule does nothing wrong; only a test that asserts the
  rewrite happened proves it does anything. A rule that rewrites a mutation clause asserts on the
  plan's structure: `LogicalWrite`'s `Display` renders neither `data` nor `cond`.
- **The corpus properties.** `exec/logical/properties.rs` — reaches a fixed point, introduces no
  write, widens no effect, binds both ways — with the rule's shapes added to `CORPUS`.
- **The differential suite, rules on and off.** The language suite run with the logical rules
  skipped answers exactly as with them run. `Rules::Skip` exists for the corpus; the suite needs a
  switch to reach it.
- **The suite is unchanged.** The language suite passes the same tests before and after.

---

## The existing rules

Sorted by what each reads, from an audit of every rule module.

| rule | logical part | physical part | before it can move |
|---|---|---|---|
| `control_flow` | all of it | — | nothing |
| `filter` | merge; push through a user projection; push into a subquery that returns its rows | the layer, frame and barrier guards, which never fire on an unresolved plan | treat every non-row-scoped parameter as permission-checked, as `column_may_raise` already does. **Bug:** the push into a subquery never checks `mentions_index_operator`, so an outer `@@` or KNN conjunct can move inside |
| `fold` | literal arithmetic; short-circuits that discard no read | every call fold (capabilities can deny any function, and folding hides `FunctionNotAllowed`); the folds that discard a read (field permissions, computed fields); parameter substitution (a value) | the clock ruling, [Non-determinism](#non-determinism) |
| `identity` | `Filter[true]`, empty `Limit`, doubled `ONLY`, a block in a block, `Filter[false]` over inert `Values`, a sort of one row whose keys read no field | `PermissionFilter[true]`; a sort of one row whose key reads a field (a computed key may raise, and dropping the sort drops its only read) | `fold_constants` must still see a `PermissionFilter[true]` before it goes |
| `kind` | casts and broadcasts over a kind the expression itself proves | kinds from a layer column or from `id` | nothing |
| `prune` | `SELECT *` over a user projection's closed schema | layer-column pruning; identity over a resolved source | a test: the logical case is untested |
| `decorrelate` | the `Filter` lift over a bare source, once joins stop hiding reads | the lift out of a `PermissionFilter` | `resolve` counts reads through a join into each input's stack. Until then, lifting before resolution leaks guarded fields and loses computed ones |
| `join` | the left/right split is syntactic | the build-side raise check reads the resolved right input | follows `decorrelate`: no join exists before one is lifted |
| `layers` | — | all of it: layers exist only after resolution | — |
| `inline` | — | all of it: function bodies are catalog | its `AUTH LIMIT` check compares against the caller's identity, which ADR 0003 keeps out of the cache key; it must compare a permission regime |

---

## The rules the architecture names

**Layer** is where the rule belongs under the criteria above. **Status** is *done* (an existing
rule does it), *partial*, or *open*. A rule that splits has a row per half. Every semantic fact a
ruling rests on was checked with a language test.

### Predicates

| rule | layer | gate, or why not | status |
|---|---|---|---|
| Filter pushdown toward the source | logical | stays within one pipeline; never across a raising conjunct | done (`filter`) |
| Conjunct splitting / filter merging | logical | a raising conjunct keeps its position relative to the rest | done (`filter`) |
| Constant folding, literal arithmetic | logical | literals only; no call (criterion 2) | partial (`fold`) |
| Boolean simplification — `x AND true`, double negation, De Morgan | logical | truthiness position only (`WHERE`, a condition): in value position `AND`/`OR` return an operand, not a boolean (`1 AND 2` is `2`, `0 OR 'x'` is `'x'`), and `!!x` is not `x` | open |
| CNF/DNF normalisation | logical | a raising conjunct may not be reordered; bound the expansion, which is exponential | open |
| Redundant predicate removal (`x > 5 AND x > 3`) | logical | both conjuncts inert. Holds across kinds because value ordering is total: checked for a string, array, object, `NONE` and `NULL` operand | open |
| Predicate subsumption across nodes | logical | as above, across stacked filters | open |
| Transitive inference (`a = b AND b > 5` ⟹ `a > 5`) | logical | adds an evaluation, so the derived conjunct must be inert. `=` is transitive across numeric kinds (`1 = 1.0 = 1dec`) and does not coerce a string (`'1' = 1` is false) | open |
| Predicate factoring | logical | reordering only among inert conjuncts | open |
| Contradiction and emptiness (`WHERE false`, `LIMIT 0`, disjoint ranges) | logical | the source is still resolved: a missing table in strict mode and a denied table still raise. The plan becomes empty rows, not no plan | partial (`identity` over `Values`) |
| Disjunction expansion (`a OR b` → union) | physical | a union changes output order, which is observable without `ORDER BY`, and dedup needs a key; the payoff is two access paths, which is a physical choice | open |
| Sargability (`x + 1 > 5` → `x > 4`) | physical | holds only for a numeric `x`: a string or array `+` is concatenation. Needs a checked kind | open |
| Inverting a function (`fn(x) = k` → `x = fn⁻¹(k)`) | physical | kind-dependent, as above | open |
| Pattern-to-range (`string::starts_with`) | physical | needs a checked `string` kind; a non-string orders by kind | open |
| Range consolidation, `IN` list → range | physical | a range admits values an `IN` list does not (`1.5` between `1` and `2`); exact only for a checked integer kind | open |
| Record-id range inference | logical | record ids are structural, so normalising id bounds into a range predicate needs no catalog. Choosing the range as an access path is physical | open |
| Table narrowing from record-id predicates | logical | drops a source whose table no id predicate admits | open |

### Projections, ordering, paging

| rule | layer | gate, or why not | status |
|---|---|---|---|
| Projection and column pruning | split | logical: a user projection's unread column, when its expression is inert and reads no field (criterion 4). Physical: a computed or guarded column | partial (`prune`) |
| Identity elimination (`WHERE true`, no-op projections) | logical | — | done (`identity`, `prune`) |
| Nested `DISTINCT` | logical | — | open |
| Chained `LIMIT` / `START` | logical | compose exactly: `START a LIMIT b` over `START c LIMIT d` | open |
| Successive `ORDER BY` | logical | the sort is stable, so an inner order shows through the outer's ties and the inner sort cannot simply go. The sound rewrite is one sort on the outer keys then the inner keys | open |
| Sort elision where order is unobservable (`IN`, `count()`) | logical | keys read no field, or the sort's reads are kept elsewhere (criterion 4) | open |
| Limit and distinct pushdown through order-preserving operators | logical | the operators crossed must be inert: rows past the limit no longer evaluate them. Under `ORDER BY`, whether an effectful column is counted for every row sorted or only for the rows kept is not pinned by a test (`PHYSICAL_PLAN_REQUIREMENTS.md` §10) | open |
| `FETCH` above filters and limits | logical | `FETCH` resolves links for output rows only; it moves above a filter that does not read a fetched field | open |
| `ONLY` / single-row shape propagation | logical | — | partial (`identity`) |
| `VERSION` stamp propagation | logical | — | open |
| `SPLIT` after filtering | logical | a conjunct that does not read the split field moves beneath the split | open |

### Subqueries and joins

| rule | layer | gate, or why not | status |
|---|---|---|---|
| Membership subquery → semi / anti join | physical, for now | criterion 4: a join hides the key's reads from `resolve` | done, post-resolve (`decorrelate`) |
| Join-side pushdown | physical, for now | follows the join it pushes into | done, post-resolve (`join`) |
| Uncorrelated subquery hoisting | split | logical where the whole statement is read-only, calls no user function and no external effect: nothing in it can write what the subquery reads. Physical where the statement writes: the subquery sees the statement's own earlier writes (`set_subquery_sees_earlier_writes.surql`). Both need the empty-subject decision | open |
| Existence short-circuit (`LIMIT 1`) | logical | truthiness only — `(SELECT …) != []`, a condition. Not `IN`, which needs every value: `1 IN (… LIMIT 1)` and `1 IN (…)` differ. The rows past the first must be inert to evaluate | open |
| Outer-predicate pushdown into a subquery | logical | done for a subquery that returns its rows unchanged; the correlated case is open | partial (`filter`) |
| Scalar-subquery CSE | logical | read-only statement, pure subquery. Two reads of one table in a writing statement can differ | open |
| Correlation narrowing | logical | — | open |
| Aggregate-subquery recognition | logical | recognising the shape is logical; answering it from an index is physical | open |
| Correlated scalar subquery → left join | physical, for now | creates a join (criterion 4); a subquery answers an array, not a scalar | open |
| Semi-join → inner join on a unique key | split | logical on `id`, which is unique by construction; physical on a unique index | open |
| Join predicate derivation | logical | a derived conjunct is an added evaluation: inert only | open |
| Nested-`SELECT` flattening | logical | — | partial (`filter`) |
| Union branch elimination, pushdown into branches | logical | over `FROM a, b` (`UnionAll`) | open |
| Aggregate pushdown through `UNION ALL` | logical | decomposable aggregates only (`count`, `sum`, `min`, `max`) | open |

### Mutations

The architecture document says of these that "none needs the catalog". Two of them are shown to,
by language tests.

| rule | layer | gate, or why not | status |
|---|---|---|---|
| Data-clause invariant hoisting | physical | a subquery in `SET` is evaluated per subject and sees the statement's earlier writes. The row-free, table-free case is already a constant after folding and needs no rule | open |
| No-op assignment (`SET a = a`) | physical | the read goes through field permissions (`set_field_to_itself.surql`) | open |
| Assignment merging by place | logical | the later value reads nothing the earlier assignment wrote | open |
| Subject grouping | logical | keeps the written order of subjects, which a range scan does not: `UPDATE t:2, t:1` answers `[t:2, t:1]` | open |
| Unreachable write (`UPDATE … WHERE false`) | logical | the target is still resolved, as for emptiness | open |
| `RETURN NONE` output pruning | split | logical: the output projection goes. Physical: the computed fields and guards only it needed | open |
| `RETURN BEFORE` / `AFTER` / `DIFF` narrowing | physical | which images exist is the write operator's | open |
| Gate subsumption | physical | reads the table's permissions | open |
| Event-handler pruning | physical | reads the events | open |
| `ASSERT` classification | physical | `ASSERT` is a field definition | open |

### Blocks, bindings and control flow

| rule | layer | gate, or why not | status |
|---|---|---|---|
| `IF` over a literal condition | logical | — | done (`control_flow`) |
| Nested-block collapse | logical | neither block binds | done (`identity`) |
| Dead statement after `RETURN` or `THROW` | logical | — | open |
| Dead-tail after a statement that always throws | logical | — | open |
| `LET` inlining, dead-binding elimination, constant propagation | logical | the value is pure and cannot raise: a `LET` with a kind coerces, which can raise | open |
| `LET` sinking into the branch that uses it | logical | as above | open |
| Discarded-value narrowing | logical | the dropped projection must be inert and read no field (criterion 4): a discarded `SELECT` whose projection raises still raises | open |
| `FOR` over a literal bound, unrolled | logical | — | open |
| Loop-invariant hoisting out of `FOR` | split | logical for an expression that is pure and reads no table; physical where it reads a table the body writes | open |
| Common-prefix hoisting out of both branches | logical | the prefix is pure | open |
| Sequence flattening | logical | — | open |
| Short-circuit ordering in `a AND b` | physical | the order is a cost choice; the logical layer may only establish that both are inert | open |
| Loop fusion | logical | effects of the two bodies do not interleave observably | open |

`CONTENT` folding is not listed: the grammar does not admit `CONTENT` together with `SET`.

### Physical, though the list files them as logical

`LIVE` query rewriting, event-handler inlining, and the reordering and cost-folding of permission
and `VALUE` clauses all read the catalog. They go to the physical rule list.

---

## Divergences the rules must not build on

Places where the language's answer was not asserted by any test. Each is settled and pinned by a
test:

| behaviour | ruling | pinned by |
|---|---|---|
| `time::now()` in a projection or a data clause | per call, per row; one value only in a `WHERE` (ADR 0006) | `language/functions/time/now_evaluated_per_call.surql` |
| reading a record through a table that does not exist — a `FROM` array or mock, a link followed in value position, `FETCH` | raises `TbNotFound`; an existing table with no such record reads as no row or `NONE` | `language/statements/select/record_read_through_missing_table.surql` |

Not yet checked: a record dereferenced inside a permission predicate reads through
`fetch_record_no_perms`, which still answers `NONE` for a missing table.

---

## Order of work

1. **Split `bind`.** Run the logical rows of [The existing rules](#the-existing-rules) before
   `resolve`, and everything else after it, where it runs today. This is what isolates the layer;
   it needs no physical plan.
2. **Fix what the split exposes:** `filter`'s index-operator check; the conservative parameter
   classification; the unchecked dereference inside a permission predicate.
3. **The differential switch**, so the language suite runs with the logical rules off.
4. **`resolve` counts reads through a join.** It moves `decorrelate` and `join` to the logical
   layer, where the architecture puts them.
5. **The open logical rows**, cheapest and most certain first: the block and binding rules, then
   the paging and ordering collapses, then predicate normalisation, then the subquery rules.
