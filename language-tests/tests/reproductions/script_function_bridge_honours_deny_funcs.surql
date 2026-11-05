/**
[env.capabilities]
deny-functions = ["string::len"]

[test]
reason = """
	surrealdb.functions.* in a script dispatches built-in functions, so it must
	honour the function capability (deny-funcs) like every other call site
	(GHSA-5w6j-qm42-4rc2). A denied function must not be reachable through the
	bridge. The unfixed bridge called fnc::synchronous directly, which checks
	only the experimental gate, so this returned the length.
"""

[[test.results]]
error = true
*/

RETURN function() { return surrealdb.functions.string.len("abc"); };
