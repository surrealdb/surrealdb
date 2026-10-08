/**
[env]
imports = ["reproductions/permission_access_path_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A table permission predicate offered to the access path chooses the
	index on `tier` with the definer's bound. For a record user the scan
	beneath the permission node renders its index and bound as
	`<permission>`, in the text form and the structured form alike, while
	the shape of the plan stays visible; the rows are the permitted ones.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    PermissionFilter [ctx: Db] [predicate: <permission>]
        IndexScan [ctx: Db] [table: t, index: <permission>, access: <permission>, direction: Forward]
''''

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { predicate: '<permission>' }, children: [{ attributes: { access: '<permission>', direction: 'Forward', index: '<permission>', table: 't' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'PermissionFilter' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: t:1 }]"
*/

EXPLAIN SELECT id FROM t;
SELECT id FROM t EXPLAIN;
SELECT id FROM t ORDER BY id;
