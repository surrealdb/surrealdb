/**
[env]

[test]
reason = """
EXPLAIN over the mutating statements. UPDATE, UPSERT and DELETE choose the
access path their subjects come from as the SELECT carrying the same predicate
chooses it -- a record lookup, an index scan, an index union, a bitmap, or the
table -- and render it beneath the WriteOperator. The access path only yields
candidates: the write judges its WHERE per record, after its gate. An UPSERT of
one table reads that table's records through the path; CREATE, INSERT and
RELATE read nothing and render the WriteOperator alone. The prefix form renders
the plan as an indented text tree, the trailing form as a structured object,
and `EXPLAIN ANALYZE` runs the statement and hangs `{rows: N}` on every
operator.

`WITH NOINDEX` is accepted in the target position of a mutating statement and
forces a table scan, so every indexed shape is paired with the same write under
NOINDEX, landing in a separate field: the rows the indexed write reaches must
equal the rows the forced scan reaches.

A b-tree index column whose idiom holds `*` stores one entry per element of that
column's value, so a scan whose key range does not pin such a column reaches one
record through several entries. `Distinct`, directly above the `IndexScan`,
collapses them, and each matching record reaches the rest of the plan -- and a
mutating statement's write -- exactly once. A scan that pins the fanning column,
through an equality prefix or a single-value containment extending that prefix,
does not fan out and carries no `Distinct`.

`count()` follows the same line. Over a pinned fanning column each record has
exactly one entry in the seeked range, so the index-only fast path applies and
the plan is a bare IndexCountScan. Over an unpinned one the fast path declines
and the count is taken over the deduplicated record stream, which keeps it
equal to the forced-scan oracle.

A relative SET -- `SET n += 1` -- records how many times the statement visited
the record rather than merely which records it reached, so the fan-out shapes
are asserted with the relative form and the resulting counter.

The shapes covered: record and record-list targets, whole-table targets,
equality on a flat field, a nested field and a UNIQUE index, both columns of a
compound index, a compound prefix plus a range, single-column ranges, an IN
list, an OR of two indexed equalities, an AND of two separately indexed
columns, containment on a flatten index, and a record-id IN list.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'UPSERT' }, context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[{ id: m:1, probe: NONE }, { id: m:2, probe: NONE }, { id: m:3, probe: NONE }]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', metrics: { output_rows: 1 }, operator: 'IndexScan' }], context: 'Db', metrics: { output_rows: 1 }, operator: 'SelectProject' }], context: 'Db', metrics: { output_rows: 1 }, operator: 'WriteOperator', total_rows: 1 }"""

[[test.results]]
value = "[{ full_probe: 1, id: m:1 }, { full_probe: NONE, id: m:2 }, { full_probe: NONE, id: m:3 }]"

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: m, index: idx_name, access: = 'a', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPSERT]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: m, index: idx_name, access: = 'a', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: m, index: idx_name, access: = 'a', direction: Forward]
"'''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: CREATE]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: INSERT]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: RELATE]
''''

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { record_id: 'm:1' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { record_id: 'm:1' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { record_id: 'm:1' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { record_id: 'm:1' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'c1'", direction: 'Forward', index: 'idx_code', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'c1'", direction: 'Forward', index: 'idx_code', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 's2'", direction: 'Forward', index: 'idx_sku', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "['g1', 2]", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'UPSERT' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "['g2', 1]", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "['g1', 2]", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1', 2]", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1'] MoreThan 1", direction: 'Forward', index: 'idx_grp_sub', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "[m:2]"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { access: '>15', direction: 'Forward', index: 'idx_qty', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[m:2, m:3]"

[[test.results]]
value = "[m:2, m:3]"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ children: [{ attributes: { access: '>=10 <30', direction: 'Forward', index: 'idx_qty', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = "[m:1, m:2]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { branches: '2', table: 'm' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: "= 'c'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'UnionIndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'UPSERT' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { branches: '2', table: 'm' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: "= 'c'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'UnionIndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "name INSIDE ['a', 'c']" }, children: [{ attributes: { branches: '2', table: 'm' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }, { attributes: { access: "= 'c'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'UnionIndexScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 30', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapOr' }], context: 'Db', operator: 'BitmapScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 30', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapOr' }], context: 'Db', operator: 'BitmapScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "name = 'a' OR qty = 30" }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 30', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapOr' }], context: 'Db', operator: 'BitmapScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 10', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapAnd' }], context: 'Db', operator: 'BitmapScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "name = 'a' AND qty = 10" }, children: [{ attributes: { table: 'm' }, children: [{ children: [{ attributes: { access: "= 'a'", index: 'idx_name' }, context: 'Db', operator: 'BitmapIndexScan' }, { attributes: { access: '= 10', index: 'idx_qty' }, context: 'Db', operator: 'BitmapIndexScan' }], context: 'Db', operator: 'BitmapAnd' }], context: 'Db', operator: 'BitmapScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'x'", direction: 'Forward', index: 'idx_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'z'", direction: 'Forward', index: 'idx_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { form: 'UPSERT' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'x'", direction: 'Forward', index: 'idx_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "= 'x'", direction: 'Forward', index: 'idx_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: m, index: idx_tags, access: = 'x', direction: Forward]
"'''

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = "[m:1, m:3]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { access: "= 'x'", direction: 'Forward', index: 'idx_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'Aggregate' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: "tags = ['x', 'y']", table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "[m:1]"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: 'id INSIDE [m:1, m:3]' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "['g1', 'y']", direction: 'Forward', index: 'idx_grp_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1', 'y']", direction: 'Forward', index: 'idx_grp_tags', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: m, index: idx_grp_tags, access: ['g1', 'y'], direction: Forward]
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
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_f', table: 'f' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_f', table: 'f' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    DistinctRecords [ctx: Db]
        IndexScan [ctx: Db] [table: f, index: idx_f, access: ['g1'], direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 2}
    DistinctRecords [ctx: Db] {rows: 2}
        IndexScan [ctx: Db] [table: f, index: idx_f, access: ['g1'], direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_f', table: 'f' }, context: 'Db', metrics: { output_rows: 2 }, operator: 'IndexScan' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'DistinctRecords' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'SelectProject' }], context: 'Db', metrics: { output_rows: 2 }, operator: 'WriteOperator', total_rows: 2 }"""

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = "[f:1, f:2]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { mode: 'GROUP ALL' }, children: [{ children: [{ attributes: { access: "['g1']", direction: 'Forward', index: 'idx_f', table: 'f' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'DistinctRecords' }], context: 'Db', operator: 'Aggregate' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "['g1', 'y']", direction: 'Forward', index: 'idx_f', table: 'f' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { access: "['g1', 'y']", direction: 'Forward', index: 'idx_f', table: 'f' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: f, index: idx_f, access: ['g1', 'y'], direction: Forward]
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
value = """{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: "= 'a'", direction: 'Forward', index: 'idx_name', table: 'm' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"""

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'm' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

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
-- choice of index; `f` offers a single compound flatten index, so a predicate on
-- its first column alone cannot be answered by any other structure and the
-- fan-out is forced into the open. f:1 carries two distinct tag elements, so a
-- seek that pins only the first column reaches it through two index entries;
-- that is what makes the collapse to one record, and the entry-versus-record
-- counts around it, observable.
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
-- fixture records, so the record count is what proves the UPSERT did not run.
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
-- The prefix form renders the same plans as an indented text tree.
-- ---------------------------------------------------------------------------

EXPLAIN UPDATE m SET hit = 1 WHERE name = 'a';
EXPLAIN UPSERT m SET hit = 1 WHERE name = 'a';
EXPLAIN DELETE m WHERE name = 'a';
EXPLAIN CREATE m:100 SET name = 'q';
EXPLAIN INSERT INTO m { id: m:101 };
EXPLAIN RELATE m:1->rel->m:2;

-- ---------------------------------------------------------------------------
-- Targets. A record target and a record-list target never consult an index:
-- each named record is read by its key, as a SELECT over the same record reads
-- it through RecordIdScan.
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
-- the access the SELECT builds for the same predicate.
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
-- IN list: a union of one index scan per value, the predicate re-checked per
-- record by the write.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE name IN ['a', 'c'] EXPLAIN;
UPSERT m SET hit = 1 WHERE name IN ['a', 'c'] EXPLAIN;
SELECT id FROM m WHERE name IN ['a', 'c'] EXPLAIN;
RETURN array::sort((UPDATE m SET in_i = 1 WHERE name IN ['a', 'c'] RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET in_n = 1 WHERE name IN ['a', 'c'] RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- OR of two indexed equalities: both indexes are read and their records
-- unioned, off the table scan.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE name = 'a' OR qty = 30 EXPLAIN;
DELETE m WHERE name = 'a' OR qty = 30 EXPLAIN;
SELECT id FROM m WHERE name = 'a' OR qty = 30 EXPLAIN;
RETURN array::sort((UPDATE m SET or_i = 1 WHERE name = 'a' OR qty = 30 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET or_n = 1 WHERE name = 'a' OR qty = 30 RETURN VALUE id));

-- AND of two separately indexed columns: both indexes are intersected before
-- any record is fetched.
UPDATE m SET hit = 1 WHERE name = 'a' AND qty = 10 EXPLAIN;
SELECT id FROM m WHERE name = 'a' AND qty = 10 EXPLAIN;
RETURN array::sort((UPDATE m SET and_i = 1 WHERE name = 'a' AND qty = 10 RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET and_n = 1 WHERE name = 'a' AND qty = 10 RETURN VALUE id));

-- ---------------------------------------------------------------------------
-- Containment on the sole column of a flatten index: the write seeks idx_tags
-- for the one element, as the SELECT does.
--
-- The single value pins the index's only column, so each matching record has
-- exactly one entry inside the seeked range. No `Distinct` sits above the scan,
-- and `count()` keeps the index-only fast path -- a bare IndexCountScan with no
-- scan beneath it, whose answer still equals the forced-scan oracle.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE tags.* CONTAINS 'x' EXPLAIN;
DELETE m WHERE tags.* CONTAINS 'z' EXPLAIN;
UPSERT m SET hit = 1 WHERE tags.* CONTAINS 'x' EXPLAIN;
SELECT id FROM m WHERE tags.* CONTAINS 'x' EXPLAIN;
EXPLAIN SELECT id FROM m WHERE tags.* CONTAINS 'x';
RETURN array::sort((UPDATE m SET con_i = 1 WHERE tags CONTAINS 'x' RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET con_n = 1 WHERE tags CONTAINS 'x' RETURN VALUE id));
SELECT count() FROM m WHERE tags.* CONTAINS 'x' GROUP ALL EXPLAIN;
SELECT count() FROM m WHERE tags CONTAINS 'x' GROUP ALL;
SELECT count() FROM m WITH NOINDEX WHERE tags CONTAINS 'x' GROUP ALL;

-- Whole-array equality against a flatten index. No index answers it -- the
-- flattened layout holds one key per element and no key for the array as a
-- whole -- so the table is scanned and the row returned.
UPDATE m SET hit = 1 WHERE tags = ['x', 'y'] EXPLAIN;
SELECT id FROM m WHERE tags = ['x', 'y'] EXPLAIN;
RETURN array::sort((UPDATE m SET arr_i = 1 WHERE tags = ['x', 'y'] RETURN VALUE id));
RETURN array::sort((UPDATE m WITH NOINDEX SET arr_n = 1 WHERE tags = ['x', 'y'] RETURN VALUE id));

-- A record-id IN list is a table scan, even though each named record could be
-- fetched directly. Passing the same ids as the target reads each by its key.
UPDATE m SET hit = 1 WHERE id IN [m:1, m:3] EXPLAIN;
SELECT id FROM m WHERE id IN [m:1, m:3] EXPLAIN;

-- ---------------------------------------------------------------------------
-- Containment on a NON-first column of a compound flatten index.
--
-- On `m` the planner takes idx_grp_tags and folds the single value into the
-- access key, narrowing on both columns. The access pins the fanning column,
-- so nothing fans out and the plan carries no `Distinct`.
-- ---------------------------------------------------------------------------

UPDATE m SET hit = 1 WHERE grp = 'g1' AND tags.* CONTAINS 'y' EXPLAIN;
SELECT id FROM m WHERE grp = 'g1' AND tags.* CONTAINS 'y' EXPLAIN;
EXPLAIN SELECT id FROM m WHERE grp = 'g1' AND tags.* CONTAINS 'y';
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
-- ['g1'] prefix. Each matching record reaches the statement exactly once: a
-- `Distinct` directly above the `IndexScan` enforces it, in both the structured
-- object and the text tree, and ANALYZE separates the entries the scan reads
-- from the records that leave it.
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

-- Adding a single-value containment pins the fanning column: the planner folds
-- it into the access key and seeks ['g1', 'y'] exactly. Nothing fans out, so
-- there is no `Distinct` above the scan -- the absence of that node against its
-- presence above is the observable difference between an exact seek and a seek
-- followed by a collapse.
UPDATE f SET hit = 1 WHERE grp = 'g1' AND tags.* CONTAINS 'y' EXPLAIN;
SELECT id FROM f WHERE grp = 'g1' AND tags.* CONTAINS 'y' EXPLAIN;
EXPLAIN SELECT id FROM f WHERE grp = 'g1' AND tags.* CONTAINS 'y';
RETURN array::sort((SELECT VALUE id FROM f WHERE grp = 'g1' AND tags CONTAINS 'y'));
RETURN array::sort((UPDATE f SET pin_i = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN VALUE id));
RETURN array::sort((UPDATE f WITH NOINDEX SET pin_n = 1 WHERE grp = 'g1' AND tags CONTAINS 'y' RETURN VALUE id));
SELECT count() FROM f WHERE grp = 'g1' AND tags CONTAINS 'y' GROUP ALL;
SELECT count() FROM f WITH NOINDEX WHERE grp = 'g1' AND tags CONTAINS 'y' GROUP ALL;

-- A relative SET pins the visit count rather than the record set: its result
-- depends on the record's current value, so it separates one visit from several
-- where an idempotent write cannot. Over the ['g1'] prefix, which leaves the
-- fanning column unpinned, f:1 is reached through two index entries and its
-- counter lands on 1 -- one increment per matching record, agreeing with the
-- forced-scan oracle.
UPDATE f SET n += 1 WHERE grp = 'g1' RETURN NONE;
UPDATE f WITH NOINDEX SET n2 = (n2 ?? 0) + 1 WHERE grp = 'g1' RETURN NONE;
SELECT id, n, n2 FROM f WITH NOINDEX ORDER BY id;

-- The same statement under the containment likewise increments f:1 once.
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
-- An allow-list that excludes every usable index renders the same plan as a
-- table target, so the plan alone does not say why the scan was chosen.
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
