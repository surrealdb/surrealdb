/**
[env]
imports = ["language/statements/define/field/computed/computed_under_where_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:boss" }

[test]
reason = """
	A record user the WHERE permission admits runs the COMPUTED body: `gated`
	admits an administrator, so reading the table runs the body, which throws
	through its stored function. A hidden computed field is not evaluated
	only for a reader who cannot see it.
"""

[[test.results]]
error = "An error occurred: boom"
*/

SELECT * FROM gate;
