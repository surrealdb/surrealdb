/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	For a record user, EXPLAIN ANALYZE renders no metrics beneath a node that
	enforces a definer's declaration — a table permission or a field-guard
	layer — since the row counts there are over records the caller may not
	see: a table scan beneath `gated`'s permission would otherwise report
	the table's whole size to a caller admitted to none of it. The nodes
	above still carry their metrics, in the text form and the structured
	form alike.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 0}
    PermissionFilter [ctx: Db] [predicate: <permission>] {rows: 0}
        TableScan [ctx: Db] [table: gated, direction: Forward]

Total rows: 0''''

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { predicate: '<permission>' }, children: [{ attributes: { direction: 'Forward', table: 'gated' }, context: 'Db', operator: 'TableScan' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'PermissionFilter' }], context: 'Db', metrics: { output_rows: 0 }, operator: 'SelectProject', total_rows: 0 }"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id] {rows: 0}
    Filter [ctx: Db] [predicate: secret = 'x'] {rows: 0}
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission] {rows: 0}
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]

Total rows: 0"'''
*/

EXPLAIN ANALYZE SELECT * FROM gated;
EXPLAIN ANALYZE FORMAT JSON SELECT * FROM gated;
EXPLAIN ANALYZE SELECT id FROM t WHERE secret = 'x';
