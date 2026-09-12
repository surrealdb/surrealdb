/**
[env]
imports = ["reproductions/udf_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to udf_frame_perms: a table whose `select` guard
	calls a user-defined function plans the table permission above the scan
	with the call carried as an expression, not unfolded into the plan; a
	table whose `select` permission is NONE for the caller yields a plan that
	admits no row.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    PermissionFilter [ctx: Db] [predicate: <permission>]
        TableScan [ctx: Db] [table: doc, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    ProjectLayer [ctx: Db] [columns: active = <permission> @permission]
        PermissionFilter [ctx: Db] [predicate: <permission>]
            EmptyScan [ctx: Rt]
''''
*/

-- 0: the guard's function body reads a NONE-guarded field and admits the row.
EXPLAIN SELECT * FROM doc;

-- 1: the table itself stays closed to the record user.
EXPLAIN SELECT * FROM membership;
