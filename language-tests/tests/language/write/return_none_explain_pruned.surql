/**
[env]
imports = ["./return_none_pruned_gate_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
`EXPLAIN` renders the write operator and the plan around it, never the stages of
its per-record body, so a `RETURN NONE` write under a select clause the stages
can prove inert (`FULL`, `NONE`, a predicate over the session alone), whose
select gate is not judged, renders as the same write under `RETURN AFTER` does.
`EXPLAIN ANALYZE` counts the rows the statement returns, which is none under
`RETURN NONE`, and runs the write.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: pf_a, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: pf_a, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: pn_a, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: pa_a, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: pa_a, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 0}
    SelectProject [ctx: Db] [projections: *] {rows: 0}
        TableScan [ctx: Db] [table: pf_a, direction: Forward] {rows: 0}

Total rows: 0''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 1}
    SelectProject [ctx: Db] [projections: *] {rows: 1}
        TableScan [ctx: Db] [table: pf_a, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 0}
    SelectProject [ctx: Db] [projections: *] {rows: 0}
        TableScan [ctx: Db] [table: pn_a, direction: Forward] {rows: 0}

Total rows: 0''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 0}
    SelectProject [ctx: Db] [projections: *] {rows: 0}
        TableScan [ctx: Db] [table: pa_a, direction: Forward] {rows: 0}

Total rows: 0''''

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'pa_a' }, context: 'Db', metrics: { output_rows: 0 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'SelectProject' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'WriteOperator', total_rows: 0 }"

[[test.results]]
value = "[{ id: pf_a_state:1, n: 4 }]"

[[test.results]]
value = "[{ id: pn_a_state:1, n: 3 }]"

[[test.results]]
value = "[]"
*/

CREATE pf_a:1 SET n = 1 RETURN NONE;
CREATE pn_a:1 SET n = 1 RETURN NONE;
CREATE pa_a:1 SET n = 1 RETURN NONE;
EXPLAIN UPDATE pf_a SET n = 2 RETURN NONE;
EXPLAIN UPDATE pf_a SET n = 2 RETURN AFTER;
EXPLAIN UPDATE pn_a SET n = 2 RETURN NONE;
EXPLAIN UPDATE pa_a SET n = 2 RETURN NONE;
EXPLAIN DELETE pa_a WHERE n > 100 RETURN NONE;
EXPLAIN ANALYZE UPDATE pf_a SET n = 3 RETURN NONE;
EXPLAIN ANALYZE UPDATE pf_a SET n = 4 RETURN AFTER;
EXPLAIN ANALYZE UPDATE pn_a SET n = 3 RETURN NONE;
EXPLAIN ANALYZE UPDATE pa_a SET n = 3 RETURN NONE;
EXPLAIN ANALYZE FORMAT JSON DELETE pa_a RETURN NONE;
SELECT * FROM pf_a_state ORDER BY id;
SELECT * FROM pn_a_state ORDER BY id;
SELECT * FROM pa_a_state ORDER BY id;
