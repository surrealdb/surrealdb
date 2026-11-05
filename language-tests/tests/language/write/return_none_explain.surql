/**
[env]
imports = ["./return_none_select_gate_inert_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
`EXPLAIN` renders the write operator and the plan around it, never the stages of
its per-record body, so a write renders as it does whatever its output is, and
the select gate its stages hold (which `RETURN NONE` makes unobservable) is not
part of it. `EXPLAIN ANALYZE` counts the rows the statement returns, which is
none under `RETURN NONE`.
"""

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 0}
    SelectProject [ctx: Db] [projections: *] {rows: 0}
        TableScan [ctx: Db] [table: item, direction: Forward] {rows: 0}

Total rows: 0''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 2}
    SelectProject [ctx: Db] [projections: *] {rows: 2}
        TableScan [ctx: Db] [table: item, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', metrics: { output_rows: 0 }, operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'SelectProject' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'WriteOperator', total_rows: 0 }"

[[test.results]]
value = "[{ id: item_state:1, n: 2, owner: user:alice }, { id: item_state:2, n: 2, owner: user:bob }, { id: item_state:3, n: 2, owner: user:alice }]"
*/

EXPLAIN UPDATE item SET n = 1 RETURN NONE;
EXPLAIN UPDATE item SET n = 1 RETURN AFTER;
EXPLAIN ANALYZE UPDATE item SET n = 1 RETURN NONE;
EXPLAIN ANALYZE UPDATE item SET n = 1 RETURN AFTER;
EXPLAIN DELETE item WHERE n > 100 RETURN NONE;
EXPLAIN ANALYZE FORMAT JSON UPDATE item SET n = 2 RETURN NONE;
SELECT * FROM item_state ORDER BY id;
