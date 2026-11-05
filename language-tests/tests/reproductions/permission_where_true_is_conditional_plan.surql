/**
[env]
namespace = true
database = true
imports = ["reproductions/permission_where_true_is_conditional_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
	Plan companion to permission_where_true_is_conditional: for a
	checked caller, a table permission written as `WHERE <expr>` plans as a
	permission filter above the scan whatever the expression folds to, so
	the user's predicate and the permission both stand in the plan for a
	SELECT with a WHERE and for a bare SELECT VALUE; the plan is the same
	whether or not the user's predicate would raise when evaluated. A RETURN
	of `$auth` plans as a Return above the parameter read.
"""

[[test.results]]
value = '''
'Return [ctx: Rt]
    ExprPlan [ctx: Rt] [expr: $auth]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret - 1 = 2 AND false]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    TableScan [ctx: Db] [table: t, direction: Forward]
''''
*/

EXPLAIN RETURN $auth;

EXPLAIN SELECT id FROM t WHERE (secret - 1 = 2) AND false;

EXPLAIN SELECT VALUE id FROM t;
