/**
[env]
namespace = true
database = true
imports = ["reproductions/closure_guard_on_a_mapped_field_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A field permission on a mapped path (`items[*].secret`) guarded by `$pred()`, an invocation
whose body is nowhere in the expression, is definer code, run in a privileged frame that holds
none of the caller's bindings. The record user's `LET $pred` is such a binding, so the guard
invokes `NONE`, which fails, and neither record is read through the caller's closure: not `t:1`,
which the record refuses and the element allows, nor `t:2`. A closure cannot be stored, so no
database parameter supplies one either.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Failed to check field permission: There was a problem running the ANONYMOUS() function. 'none' is not a function"

[[test.results]]
error = "Failed to check field permission: There was a problem running the ANONYMOUS() function. 'none' is not a function"
*/

LET $pred = || allowed;
SELECT * FROM t:1;
SELECT * FROM t:2;
