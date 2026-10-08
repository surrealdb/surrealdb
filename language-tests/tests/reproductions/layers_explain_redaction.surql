/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A permission predicate and a computed body are the definer's, and a
	caller they are enforced on must not learn them from the plan: for a
	record user, EXPLAIN renders a layer and a permission filter as bare
	nodes with no column, guard or predicate text, in the text form and in
	the structured form alike. The caller's own predicate is shown.
"""

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret = 'x']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: is_owner, secret, c]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = """{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: "secret = 'x'" }, children: [{ attributes: { columns: 'secret = <permission> @permission' }, children: [{ attributes: { columns: 'is_owner = <computed> : bool @computed' }, children: [{ attributes: { direction: 'Forward', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'ProjectLayer' }], context: 'Db', operator: 'ProjectLayer' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    PermissionFilter [ctx: Db] [predicate: <permission>]
        TableScan [ctx: Db] [table: gated, direction: Forward]
''''

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { predicate: '<permission>' }, children: [{ attributes: { direction: 'Forward', table: 'gated' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'PermissionFilter' }], context: 'Db', operator: 'SelectProject' }"
*/

EXPLAIN SELECT id FROM t WHERE secret = 'x';
EXPLAIN SELECT is_owner, secret, c FROM t;
SELECT id FROM t WHERE secret = 'x' EXPLAIN;

-- The table's own permission predicate.
EXPLAIN SELECT * FROM gated;
SELECT * FROM gated EXPLAIN;
