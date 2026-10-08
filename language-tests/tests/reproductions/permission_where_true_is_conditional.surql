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
A table permission written as `WHERE <expr>` is a conditional permission
whatever the expression folds to, so for a checked caller no field read off
its rows is free: the WHERE clause is evaluated for every row the permission
admits, and `(secret - 1 = 2) AND false` raises the subtraction error rather
than short-circuiting to no rows. The row itself
stays readable.
"""

[[test.results]]
value = "user:alice"

[[test.results]]
error = "Cannot perform subtraction with 'string' and 'int'"

[[test.results]]
value = "[t:1]"
*/

RETURN $auth;

SELECT id FROM t WHERE (secret - 1 = 2) AND false;

SELECT VALUE id FROM t;
