/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
EXPLAIN over the mutating statements. UPDATE, UPSERT and DELETE are planned by
the legacy planner and render the legacy plan shape -- an array of
{ operation, detail } entries naming Iterate Record / Iterate Table / Iterate
Index -- while the SELECT carrying the same predicate is planned by the
streaming planner. Three renderings are in play and each shows something the
others do not: the trailing form on a mutating statement gives the legacy array,
the trailing form on a SELECT gives the streaming plan as a structured object,
and the prefix form on a SELECT gives the same plan as an indented text tree.
`EXPLAIN ANALYZE` in the prefix position runs the statement and hangs a
`{rows: N}` on every operator; `EXPLAIN FULL` is the trailing-form equivalent
and appends the execution operators to the legacy array. The file runs the two
planners side by side over one index set so that the places where they differ in
index capability are recorded rather than assumed. Only the streaming strategies
are pinned: compute-only renders the legacy shape for the SELECT halves too, so
the same file cannot assert both, and the two streaming strategies plan
identically here.

`WITH NOINDEX` is accepted in the target position of a mutating statement, which
makes a forced table scan available as a row oracle on the write path itself:
every indexed shape is paired with the same write under NOINDEX, landing in a
separate field so the two do not disturb each other. A plan that looks right
while touching a different set of records from the oracle is what this file
exists to catch.

A b-tree index column whose idiom holds `*` stores one entry per element of that
column's value, so a scan whose key range does not pin such a column reaches one
record through several entries. `Distinct`, sitting directly above the
`IndexScan` and below any residual `Filter`, collapses them, and each matching
record therefore reaches the rest of the plan -- and a mutating statement's
write -- exactly once. Under `EXPLAIN ANALYZE` the scan's own `{rows: N}` counts
entries while the `Distinct` above it counts records, so one plan carries both
numbers; on the legacy path, which names no `Distinct`, the record count is
EXPLAIN FULL's Fetch count and the entry count is what the seek reads. A scan
that does pin the fanning column, either through an equality prefix or through a
single-value containment extending that prefix, does not fan out and carries no
`Distinct`. The presence
or absence of that node is asserted on both sides of the contrast, because it is
the observable difference between an exact seek and a seek followed by a
collapse.

`count()` follows the same line. Over a pinned fanning column each record has
exactly one entry in the seeked range, so the index-only fast path applies and
the plan is a bare IndexCountScan. Over an unpinned one the fast path declines
and the count is taken over the deduplicated record stream, which is what keeps
it equal to the forced-scan oracle.

A relative SET -- `SET n += 1` -- is the sharpest assertion the write path
offers, because its result depends on the record's current value and so records
how many times the statement visited the record rather than merely which records
it reached. Idempotent forms (a DELETE, or a SET to a constant) cannot
distinguish one visit from several, so the fan-out shapes are asserted with the
relative form and the resulting counter.

Four divergences between the two planners are pinned, in order of severity.

1. DEFECT, silent whole-table scan. Containment on the sole column of a flatten
   index -- `tags CONTAINS 'x'` against an index on `tags.*` -- is answered by
   the legacy planner with Iterate Table, while the streaming planner seeks the
   index for the element. Rows agree with the oracle, so nothing is wrong with
   the answer; an UPDATE or DELETE simply reads every record in the table where
   the equivalent SELECT reads one key range. Correct behaviour is the index
   seek both planners have available.

2. Narrower index choice. With both idx_grp_sub and idx_grp_tags defined,
   `grp = 'g1' AND tags CONTAINS 'y'` makes the legacy planner seek idx_grp_sub
   on grp alone and re-check the containment per record, where the streaming
   planner seeks idx_grp_tags on both columns. Rows are correct; the scan is
   wider than it needs to be.

3. A compound prefix the containment does not extend. On `f`, whose only index
   is the compound flatten idx_f, the streaming planner folds a single-value
   containment on the fanning column into the access key and seeks ['g1', 'y']
   exactly, while the legacy planner seeks the ['g1'] prefix, reads every entry
   under it and re-checks the containment per record. Rows are correct, and the
   wider seek is the one that has to collapse duplicates. Distinct from 2:
   there is no other index to choose here, so this is the access key the chosen
   index is given rather than which index is chosen.

4. One index instead of two. For an AND of two separately indexed columns the
   legacy planner seeks one index and re-checks the other conjunct, where the
   streaming planner intersects both through a bitmap. Rows are correct.

Shapes that agree are asserted too, so a regression in them is visible: record
and record-list targets, whole-table targets, equality on a flat field, a nested
field and a UNIQUE index, both columns of a compound index, a compound prefix
plus a range, single-column ranges, an IN list, and an OR of two indexed
equalities -- which the legacy planner answers with one Iterate Index per branch
rather than falling back to a table scan. A record-id IN list is a table scan on
both paths, even though each named record could be fetched directly; passing the
same ids as the target instead reaches Iterate Record.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { table: 'm' }, operation: 'Iterate Yield' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: m:1, probe: NONE }, { id: m:2, probe: NONE }, { id: m:3, probe: NONE }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: '=', value: 'a' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 1 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ full_probe: 1, id: m:1 }, { full_probe: NONE, id: m:2 }, { full_probe: NONE, id: m:3 }]"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
error = "Invalid statement: EXPLAIN is only supported with the new execution model"

[[test.results]]
value = "[{ detail: { record: m:1 }, operation: 'Iterate Record' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "{ children: [{ attributes: { record_id: 'm:1' }, context: 'Db', expressions: [{ role: 'record_id', sql: 'm:1' }], operator: 'RecordIdScan' }], context: 'Db', expressions: [{ role: 'field', sql: 'id' }], operator: 'Project' }"

[[test.results]]
value = "[{ detail: { record: m:1 }, operation: 'Iterate Record' }, { detail: { record: m:3 }, operation: 'Iterate Record' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { record: m:1 }, operation: 'Iterate Record' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { record: m:1 }, operation: 'Iterate Record' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: '=', value: 'a' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', predicate: "name = 'a'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_code', operator: '=', value: 'c1' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'c1'", direction: 'Forward', index: 'idx_code', predicate: "meta.code = 'c1'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_sku', operator: '=', value: 's2' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_grp_sub', operator: '=', value: ['g1', 2] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_grp_sub', operator: '=', value: ['g2', 1] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_grp_sub', operator: '=', value: ['g1', 2] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1', 2]", direction: 'Forward', index: 'idx_grp_sub', predicate: "grp = 'g1' AND sub = 2" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_grp_sub', prefix: ['g1'], ranges: [{ operator: '>', value: 1 }] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_grp_sub', prefix: ['g1'], ranges: [{ operator: '>', value: 1 }] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1'] MoreThan 1", direction: 'Forward', index: 'idx_grp_sub', predicate: "grp = 'g1' AND sub > 1" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 15 }, index: 'idx_qty', to: { inclusive: false, value: NONE } }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { access: '>15', direction: 'Forward', index: 'idx_qty', predicate: 'qty > 15' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[m:2, m:3]"

[[test.results]]
value = "[m:2, m:3]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 10 }, index: 'idx_qty', to: { inclusive: false, value: 30 } }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: 'union', value: ['a', 'c'] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: 'union', value: ['a', 'c'] }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "name INSIDE ['a', 'c']" }, children: [{ attributes: { branches: '2', table: 'm' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: "= 'c'", direction: 'Forward', index: 'idx_name' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'UnionIndexScan' }], context: 'Db', expressions: [{ role: 'predicate', sql: "name INSIDE ['a', 'c']" }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: '=', value: 'a' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'idx_qty', operator: '=', value: 30 }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: '=', value: 'a' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { plan: { index: 'idx_qty', operator: '=', value: 30 }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "name = 'a' OR qty = 30" }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 30', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapOr' }], context: 'Db', operator: 'BitmapResolve' }], context: 'Db', expressions: [{ role: 'predicate', sql: "name = 'a' OR qty = 30" }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_qty', operator: '=', value: 10 }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "name = 'a' AND qty = 10" }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 10', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapAnd' }], context: 'Db', operator: 'BitmapResolve' }], context: 'Db', expressions: [{ role: 'predicate', sql: "name = 'a' AND qty = 10" }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'x'", direction: 'Forward', index: 'idx_tags', predicate: "tags CONTAINS 'x'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_tags, access: = 'x', direction: Forward, predicate: tags CONTAINS 'x']
"'''

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = """{ attributes: { condition: "tags CONTAINS 'x'", source: 'm' }, context: 'Db', expressions: [{ role: 'source', sql: 'm' }, { role: 'predicate', sql: "tags CONTAINS 'x'" }], operator: 'IndexCountScan' }"""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: "tags = ['x', 'y']", table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: 'id INSIDE [m:1, m:3]', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_grp_sub', operator: '=', value: 'g1' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1', 'y']", direction: 'Forward', index: 'idx_grp_tags', predicate: "grp = 'g1' AND tags CONTAINS 'y'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_grp_tags, access: ['g1', 'y'], direction: Forward, predicate: grp = 'g1' AND tags CONTAINS 'y']
"'''

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_f', operator: '=', value: 'g1' }, table: 'f' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_f', predicate: "grp = 'g1'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [index: idx_f, access: ['g1'], direction: Forward, predicate: grp = 'g1']
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 2}
    DistinctRecords [ctx: Db] {rows: 2}
        IndexScan [ctx: Db] [index: idx_f, access: ['g1'], direction: Forward, predicate: grp = 'g1'] {rows: 3}

Total rows: 2"'''

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_f', operator: '=', value: 'g1' }, table: 'f' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = """{ attributes: { mode: 'GROUP ALL' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_f', predicate: "grp = 'g1'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', expressions: [{ role: 'agg_arg', sql: 'NONE' }], operator: 'Aggregate' }"""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_f', operator: '=', value: 'g1' }, table: 'f' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1', 'y']", direction: 'Forward', index: 'idx_f', predicate: "grp = 'g1' AND tags CONTAINS 'y'" }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [index: idx_f, access: ['g1', 'y'], direction: Forward, predicate: grp = 'g1' AND tags CONTAINS 'y']
"'''

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: f:1, n: 1, n2: 1 }, { id: f:2, n: 1, n2: 1 }, { id: f:3, n: 0, n2: NONE }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 1, c2: 1, id: f:1 }, { c: 1, c2: 1, id: f:2 }, { c: NONE, c2: NONE, id: f:3 }]"

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = "[f:3]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx_name', operator: '=', value: 'a' }, table: 'm' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'm' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:2]"
*/

-- The two tables carry deliberately different index sets. `m` offers the planner a
-- choice, so it records which index the legacy planner reaches for; `f` offers a
-- single compound flatten index, so a predicate on its first column alone cannot
-- be answered by any other structure and the fan-out is forced into the open.
-- f:1 carries two distinct tag elements, so a seek that pins only the first
-- column reaches it through two index entries; that is what makes the collapse
-- to one record, and the entry-versus-record counts around it, observable.
{
    DEFINE INDEX idx_name ON m FIELDS name;
    DEFINE INDEX idx_qty ON m FIELDS qty;
    DEFINE INDEX idx_grp_sub ON m FIELDS grp, sub;
    DEFINE INDEX idx_tags ON m FIELDS tags.*;
    DEFINE INDEX idx_grp_tags ON m FIELDS grp, tags.*;
    DEFINE INDEX idx_code ON m FIELDS meta.code;
    DEFINE INDEX idx_sku ON m FIELDS sku UNIQUE;
    CREATE m:1 SET name = 'a', qty = 10, grp = 'g1', sub = 1, tags = ['x', 'y'], meta = { code: 'c1' }, sku = 's1';
    CREATE m:2 SET name = 'b', qty = 20, grp = 'g1', sub = 2, tags = ['y'], meta = { code: 'c2' }, sku = 's2';
    CREATE m:3 SET name = 'c', qty = 30, grp = 'g2', sub = 1, tags = ['x', 'z'], meta = { code: 'c1' }, sku = 's3';

    DEFINE INDEX idx_f ON f FIELDS grp, tags.*;
    CREATE f:1 SET grp = 'g1', tags = ['x', 'y'], n = 0;
    CREATE f:2 SET grp = 'g1', tags = ['y'], n = 0;
    CREATE f:3 SET grp = 'g2', tags = ['y'], n = 0;
    RETURN 'OK';
};

-- ---------------------------------------------------------------------------
-- A trailing EXPLAIN plans but does not execute.
--
-- Everything below this point depends on it: the file asserts plans for writes
-- against a fixture it expects to stay untouched. `probe` must be absent from
-- every record afterwards, and the table must still hold exactly the three
-- fixture records -- an UPSERT on a table target with no WHERE clause plans
-- Iterate Yield because it would create a record rather than visit existing
-- ones, so the record count is what proves it did not run.
-- ---------------------------------------------------------------------------

UPDATE m SET probe = 'update' EXPLAIN;
UPSERT m SET probe = 'upsert' EXPLAIN;
DELETE m EXPLAIN;
SELECT id, probe FROM m WITH NOINDEX ORDER BY id;

-- EXPLAIN FULL is the opposite: it runs the statement and appends the execution
-- operators. On a mutating statement that means the write lands. Asserted with a
-- field of its own so the rest of the file stays readable.
UPDATE m SET full_probe = 1 WHERE name = 'a' EXPLAIN FULL;
SELECT id, full_probe FROM m WITH NOINDEX ORDER BY id;

-- ---------------------------------------------------------------------------
-- Grammar boundary: the prefix form is rejected at runtime on every mutating
-- statement. The trailing form is the only one that reaches a planner, which is
-- why this file uses it throughout.
-- ---------------------------------------------------------------------------

EXPLAIN UPDATE m SET hit = 1 WHERE name = 'a';
EXPLAIN UPSERT m SET hit = 1 WHERE name = 'a';
EXPLAIN DELETE m WHERE name = 'a';
EXPLAIN CREATE m:100 SET name = 'q';
EXPLAIN INSERT INTO m { id: m:101 };
EXPLAIN RELATE m:1->rel->m:2;

-- ---------------------------------------------------------------------------
-- Targets. A record target and a record-list target never consult an index; the
-- legacy planner emits one Iterate Record per named record. A SELECT over the
-- same record reaches it through RecordIdScan, so the two agree on the shape of
-- the work while naming the operator differently.
-- ---------------------------------------------------------------------------

UPDATE m:1 SET hit = 1 EXPLAIN;
SELECT id FROM m:1 EXPLAIN;
UPDATE [m:1, m:3] SET hit = 1 EXPLAIN;
DELETE m:1 EXPLAIN;

-- A record target with a WHERE clause still iterates the record and re-checks
-- the predicate, so a non-matching predicate updates nothing.
UPDATE m:1 SET hit = 1 WHERE name = 'b' EXPLAIN;
RETURN array::sort((UPDATE m:1 SET t_rec = 1 WHERE name = 'b' RETURN VALUE id));
RETURN array::sort((UPDATE m:1 SET t_rec = 1 WHERE name = 'a' RETURN VALUE id));

-- A whole-table target scans, as SELECT does.
UPDATE m SET hit = 1 EXPLAIN;
SELECT id FROM m EXPLAIN;

-- ---------------------------------------------------------------------------
-- Equality on a single indexed column. Matches SELECT: both seek the index.
--
-- `WITH NOINDEX` is accepted on a mutating statement in the target position, so
-- the forced table scan is available as a row oracle on the write path itself.
-- Every indexed shape below is paired with it, with the two writes landing in
-- different fields so neither disturbs the other.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE name = 'a' EXPLAIN;
SELECT id FROM m WHERE name = 'a' EXPLAIN;
RETURN array::sort((UPDATE m SET eq_i = 1 WHERE name = 'a' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET eq_n = 1 WHERE name = 'a' RETURN VALUE id));

-- Equality on a nested field, and on a UNIQUE index. Both match SELECT.
UPDATE m SET hit = 1 WHERE meta.code = 'c1' EXPLAIN;
SELECT id FROM m WHERE meta.code = 'c1' EXPLAIN;
RETURN array::sort((UPDATE m SET nest_i = 1 WHERE meta.code = 'c1' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET nest_n = 1 WHERE meta.code = 'c1' RETURN VALUE id));

UPDATE m SET hit = 1 WHERE sku = 's2' EXPLAIN;
RETURN array::sort((UPDATE m SET uniq_i = 1 WHERE sku = 's2' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET uniq_n = 1 WHERE sku = 's2' RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- Compound index, both columns equal: one exact seek on the composite key.
-- Matches SELECT.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE grp = 'g1' AND sub = 2 EXPLAIN;
UPSERT m SET hit = 1 WHERE grp = 'g2' AND sub = 1 EXPLAIN;
DELETE m WHERE grp = 'g1' AND sub = 2 EXPLAIN;
SELECT id FROM m WHERE grp = 'g1' AND sub = 2 EXPLAIN;
RETURN array::sort((UPDATE m SET cmp_i = 1 WHERE grp = 'g1' AND sub = 2 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET cmp_n = 1 WHERE grp = 'g1' AND sub = 2 RETURN VALUE id));

-- Compound prefix plus a range on the second column: prefix + ranges, matching
-- the access the streaming planner builds for the same predicate.
UPDATE m SET hit = 1 WHERE grp = 'g1' AND sub > 1 EXPLAIN;
DELETE m WHERE grp = 'g1' AND sub > 1 EXPLAIN;
SELECT id FROM m WHERE grp = 'g1' AND sub > 1 EXPLAIN;
RETURN array::sort((UPDATE m SET pre_i = 1 WHERE grp = 'g1' AND sub > 1 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET pre_n = 1 WHERE grp = 'g1' AND sub > 1 RETURN VALUE id));

-- A range on a single indexed column, half-open and bounded on both sides.
UPDATE m SET hit = 1 WHERE qty > 15 EXPLAIN;
SELECT id FROM m WHERE qty > 15 EXPLAIN;
RETURN array::sort((UPDATE m SET hrng_i = 1 WHERE qty > 15 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET hrng_n = 1 WHERE qty > 15 RETURN VALUE id));
UPDATE m SET hit = 1 WHERE qty >= 10 AND qty < 30 EXPLAIN;
RETURN array::sort((UPDATE m SET rng_i = 1 WHERE qty >= 10 AND qty < 30 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET rng_n = 1 WHERE qty >= 10 AND qty < 30 RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- IN list. The legacy planner folds the list into a single union seek on one
-- index; the streaming planner builds a UnionIndexScan of one scan per value and
-- re-checks the predicate above it. Different shapes, same index, same rows.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE name IN ['a', 'c'] EXPLAIN;
UPSERT m SET hit = 1 WHERE name IN ['a', 'c'] EXPLAIN;
SELECT id FROM m WHERE name IN ['a', 'c'] EXPLAIN;
RETURN array::sort((UPDATE m SET in_i = 1 WHERE name IN ['a', 'c'] RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET in_n = 1 WHERE name IN ['a', 'c'] RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- OR of two indexed equalities. The legacy planner emits one Iterate Index per
-- branch and unions the records it iterates; the streaming planner resolves the
-- same two indexes through a bitmap union. Both stay off the table scan.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE name = 'a' OR qty = 30 EXPLAIN;
DELETE m WHERE name = 'a' OR qty = 30 EXPLAIN;
SELECT id FROM m WHERE name = 'a' OR qty = 30 EXPLAIN;
RETURN array::sort((UPDATE m SET or_i = 1 WHERE name = 'a' OR qty = 30 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET or_n = 1 WHERE name = 'a' OR qty = 30 RETURN VALUE id));

-- AND of two separately indexed columns. Here the two planners genuinely differ
-- in capability: the legacy planner picks ONE index and re-checks the other
-- conjunct per fetched record, while the streaming planner intersects both
-- indexes before fetching anything.
UPDATE m SET hit = 1 WHERE name = 'a' AND qty = 10 EXPLAIN;
SELECT id FROM m WHERE name = 'a' AND qty = 10 EXPLAIN;
RETURN array::sort((UPDATE m SET and_i = 1 WHERE name = 'a' AND qty = 10 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET and_n = 1 WHERE name = 'a' AND qty = 10 RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- Containment on the sole column of a flatten index. This is the asymmetry that
-- costs production latency: SELECT seeks idx_tags for one element, the mutating
-- path scans the whole table. Rows agree, cost does not.
--
-- The single value pins the index's only column, so each matching record has
-- exactly one entry inside the seeked range. No `Distinct` sits above the scan,
-- and `count()` keeps the index-only fast path -- a bare IndexCountScan with no
-- scan beneath it, whose answer still equals the forced-scan oracle.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE tags CONTAINS 'x' EXPLAIN;
DELETE m WHERE tags CONTAINS 'z' EXPLAIN;
UPSERT m SET hit = 1 WHERE tags CONTAINS 'x' EXPLAIN;
SELECT id FROM m WHERE tags CONTAINS 'x' EXPLAIN;
EXPLAIN SELECT id FROM m WHERE tags CONTAINS 'x';
RETURN array::sort((UPDATE m SET con_i = 1 WHERE tags CONTAINS 'x' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET con_n = 1 WHERE tags CONTAINS 'x' RETURN VALUE id));
SELECT count() FROM m WHERE tags CONTAINS 'x' GROUP ALL EXPLAIN;
SELECT count() FROM m WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM m WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

-- Whole-array equality against a flatten index. Neither planner claims the
-- index for it -- the flattened layout holds one key per element and no key for
-- the array as a whole -- so both scan and both return the row.
UPDATE m SET hit = 1 WHERE tags = ['x', 'y'] EXPLAIN;
SELECT id FROM m WHERE tags = ['x', 'y'] EXPLAIN;
RETURN array::sort((UPDATE m SET arr_i = 1 WHERE tags = ['x', 'y'] RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET arr_n = 1 WHERE tags = ['x', 'y'] RETURN VALUE id));

-- A record-id IN list is a table scan on both paths, even though each named
-- record could be fetched directly. Passing the same ids as the target instead
-- of as a predicate is what reaches Iterate Record.
UPDATE m SET hit = 1 WHERE id IN [m:1, m:3] EXPLAIN;
SELECT id FROM m WHERE id IN [m:1, m:3] EXPLAIN;

-- ---------------------------------------------------------------------------
-- Containment on a NON-first column of a compound flatten index.
--
-- On `m` the legacy planner has a choice and takes idx_grp_sub, narrowing on grp
-- alone and re-checking the containment per record; the streaming planner takes
-- idx_grp_tags and folds the single value into the access key, narrowing on both
-- columns. Coarser, but correct.
--
-- Both chosen accesses pin whatever fanning column they touch -- idx_grp_sub has
-- none, and the streaming access pins idx_grp_tags's -- so neither side fans out
-- and the streaming plan carries no `Distinct`.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' EXPLAIN;
SELECT id FROM m WHERE grp = 'g1' AND tags CONTAINS 'y' EXPLAIN;
EXPLAIN SELECT id FROM m WHERE grp = 'g1' AND tags CONTAINS 'y';
RETURN array::sort((UPDATE m SET ncon_i = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET ncon_n = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN VALUE id));
SELECT count() FROM m WHERE grp = 'g1' AND tags CONTAINS 'y' GROUP ALL;
SELECT count() FROM m WITH NOINDEX WHERE grp = 'g1' AND tags CONTAINS 'y' GROUP ALL;
RETURN array::sort((UPDATE m SET ncon2_i = 1 WHERE grp = 'g1' AND tags CONTAINS 'x' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET ncon2_n = 1 WHERE grp = 'g1' AND tags CONTAINS 'x' RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- The same shape on `f`, where the compound flatten index is the only one on
-- offer, so a scan cannot sidestep the fan-out by choosing another structure.
--
-- Seeked on its first column alone, idx_f leaves its fanning column unpinned:
-- f:1 holds two tag elements and is reached through two entries under the
-- ['g1'] prefix. Each matching record reaches the statement exactly once.
-- The streaming plan shows what enforces that -- a `Distinct` directly above the
-- `IndexScan` -- in both the structured object and the text tree, and ANALYZE
-- separates the three entries the scan reads from the two records that leave it.
-- The legacy array names only the seek, so on that path the same property is
-- asserted by EXPLAIN FULL's Fetch count and by the counters further down.
-- ---------------------------------------------------------------------------

UPDATE f SET hit = 1 WHERE grp = 'g1' EXPLAIN;
SELECT id FROM f WHERE grp = 'g1' EXPLAIN;
EXPLAIN SELECT id FROM f WHERE grp = 'g1';
EXPLAIN ANALYZE SELECT id FROM f WHERE grp = 'g1';
UPDATE f SET hit = 1 WHERE grp = 'g1' EXPLAIN FULL;
RETURN array::sort((UPDATE f SET fan_i = 1 WHERE grp = 'g1' RETURN VALUE id));
RETURN array::sort((UPDATE f WITH NOINDEX SET fan_n = 1 WHERE grp = 'g1' RETURN VALUE id));
-- An unpinned fanning column takes `count()` off the index-only fast path: the
-- plan aggregates over the deduplicated record stream rather than counting index
-- entries, so the indexed count equals the forced-scan oracle.
SELECT count() FROM f WHERE grp = 'g1' GROUP ALL EXPLAIN;
SELECT count() FROM f WHERE grp = 'g1' GROUP ALL;
SELECT count() FROM f WITH NOINDEX WHERE grp = 'g1' GROUP ALL;

-- Adding a single-value containment pins the fanning column for the streaming
-- planner, which folds it into the access key and seeks ['g1', 'y'] exactly.
-- Nothing fans out, so there is no `Distinct` above the scan -- the absence of
-- that node against its presence above is the observable difference between an
-- exact seek and a seek followed by a collapse. The legacy planner keeps the
-- ['g1'] prefix and re-checks the containment per record, so its seek is the
-- wider of the two.
UPDATE f SET hit = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' EXPLAIN;
SELECT id FROM f WHERE grp = 'g1' AND tags CONTAINS 'y' EXPLAIN;
EXPLAIN SELECT id FROM f WHERE grp = 'g1' AND tags CONTAINS 'y';
RETURN array::sort((SELECT VALUE id FROM f WHERE grp = 'g1' AND tags CONTAINS 'y'));
RETURN array::sort((UPDATE f SET pin_i = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN VALUE id));
RETURN array::sort((UPDATE f WITH NOINDEX SET pin_n = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN VALUE id));
SELECT count() FROM f WHERE grp = 'g1' AND tags CONTAINS 'y' GROUP ALL;
SELECT count() FROM f WITH NOINDEX WHERE grp = 'g1' AND tags CONTAINS 'y' GROUP ALL;

-- A relative SET pins the visit count rather than the record set: its result
-- depends on the record's current value, so it separates one visit from several
-- where an idempotent write cannot. Over the ['g1'] prefix, which leaves the
-- fanning column unpinned on both planners, f:1 is reached through two index
-- entries and its counter lands on 1 -- one increment per matching record,
-- agreeing with the forced-scan oracle.
UPDATE f SET n += 1 WHERE grp = 'g1' RETURN NONE;
UPDATE f WITH NOINDEX SET n2 = (n2 ?? 0) + 1 WHERE grp = 'g1' RETURN NONE;
SELECT id, n, n2 FROM f WITH NOINDEX ORDER BY id;

-- The same statement under the containment, which the legacy planner answers
-- with the prefix seek all the same, likewise increments f:1 once.
UPDATE f SET c = (c ?? 0) + 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN NONE;
UPDATE f WITH NOINDEX SET c2 = (c2 ?? 0) + 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN NONE;
SELECT id, c, c2 FROM f WITH NOINDEX ORDER BY id;

-- DELETE reaches each matching record once as well, so RETURN BEFORE yields one
-- row per record rather than one per index entry.
RETURN array::sort((DELETE f WHERE grp = 'g1' RETURN BEFORE).id);
SELECT VALUE id FROM f WITH NOINDEX;

-- ---------------------------------------------------------------------------
-- The index allow-list is honoured on the mutating path: naming an index that
-- cannot serve the predicate falls back to a table scan rather than erroring.
-- Only WITH NOINDEX carries a Fallback entry naming the reason; an allow-list
-- that happens to exclude every usable index renders the same plan as a table
-- target, so the plan alone does not say why the scan was chosen.
-- ---------------------------------------------------------------------------

UPDATE m WITH INDEX idx_name SET hit = 1 WHERE name = 'a' EXPLAIN;
UPDATE m WITH INDEX idx_qty SET hit = 1 WHERE name = 'a' EXPLAIN;
UPDATE m WITH NOINDEX SET hit = 1 WHERE name = 'a' EXPLAIN;
DELETE m WITH NOINDEX WHERE name = 'a' EXPLAIN;

-- An allow-list narrows which index may be consulted, never which records
-- qualify, so both spellings must still touch exactly the record the predicate
-- selects -- the one served through idx_name and the one reached by the
-- fallback scan alike.
RETURN array::sort((UPDATE m WITH INDEX idx_name SET wi_i = 1 WHERE name = 'a' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH INDEX idx_qty SET wi_f = 1 WHERE name = 'a' RETURN VALUE id));

-- The whole-table scan the mutating path falls back to for flatten containment
-- still has to delete exactly the records the predicate selects, once each:
-- m:1 and m:3 hold 'x', m:2 does not.
RETURN array::sort((DELETE m WHERE tags CONTAINS 'x' RETURN BEFORE).id);
SELECT VALUE id FROM m WITH NOINDEX ORDER BY id;
