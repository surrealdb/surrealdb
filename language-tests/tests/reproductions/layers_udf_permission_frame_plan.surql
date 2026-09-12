/**
[env]
imports = ["reproductions/layers_udf_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to layers_udf_permission_frame: a table whose
	`select` guard calls a user-defined function plans the table permission
	above the scan with the guard's expression carried as a call, not
	unfolded; a top-level RETURN of the same call plans as a Return above the
	call expression. Over `membership`, `SELECT *` and an aliased projection
	of `active` and `tier` stack the computed layer for `tier` beneath the
	guard layer for `active` and `tier`.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    PermissionFilter [ctx: Db] [predicate: <permission>]
        TableScan [ctx: Db] [table: doc, direction: Forward]
''''

[[test.results]]
value = '''
'Return [ctx: Db]
    Expr [ctx: Db] [expr: fn::tier_of(user:reader)]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    ProjectLayer [ctx: Db] [columns: active = <permission> @permission, tier = <permission> @permission]
        TableScan [ctx: Db] [table: membership, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: who, active AS a, tier AS t]
    ProjectLayer [ctx: Db] [columns: active = <permission> @permission, tier = <permission> @permission]
        TableScan [ctx: Db] [table: membership, direction: Forward]
''''
*/

-- The guard's function reads `tier`, derived from `active`, and admits the row.
EXPLAIN SELECT * FROM doc;

-- Called by the caller, the function reads the guarded row: `tier` is NONE.
EXPLAIN RETURN fn::tier_of(user:reader);

-- The caller's own reads are guarded: `active` and `tier` are cut.
EXPLAIN SELECT * FROM membership;
EXPLAIN SELECT who, active AS a, tier AS t FROM membership;
