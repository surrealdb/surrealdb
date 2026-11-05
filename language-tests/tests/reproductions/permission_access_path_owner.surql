/**
[env]
imports = ["reproductions/permission_access_path_import.surql"]
auth = { level = "owner" }

[test]
reason = """
	The owner sibling of permission_access_path_redaction: a caller
	the permission is not enforced on has no permission node in the plan and
	reads the table whole, so nothing is redacted.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"
*/

EXPLAIN SELECT id FROM t;
SELECT id FROM t ORDER BY id;
