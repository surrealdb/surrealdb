/**
[env]
imports = ["reproductions/planned_source_enforced_once_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	The plan for a record user over a table with a conditional table
	permission and a field guard: both stand above the scan — the
	`PermissionFilter` and the layer setting `hidden` — and the scan beneath
	them is a plain `TableScan`, planned with a table context that tells it
	the plan enforces, not a runtime-resolving scan that would judge the rows
	a second time beneath those nodes.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    ProjectLayer [ctx: Db] [columns: hidden = <permission> @permission]
        PermissionFilter [ctx: Db] [predicate: <permission>]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: hidden = 'theirs']
        ProjectLayer [ctx: Db] [columns: hidden = <permission> @permission]
            PermissionFilter [ctx: Db] [predicate: <permission>]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''
*/

EXPLAIN SELECT * FROM t ORDER BY id;
EXPLAIN SELECT id FROM t WHERE hidden = 'theirs';
