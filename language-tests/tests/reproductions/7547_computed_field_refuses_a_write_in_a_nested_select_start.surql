/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A computed field refuses a write in the `START` clause of a nested `SELECT`
	as it refuses one anywhere else in its body. The request is refused at plan
	time, at the `DEFINE FIELD` (statement 2), before any statement of it runs.
"""
issue = 7547

[test.results]
request-error = "Invalid statement: a write is not allowed in a COMPUTED clause (statement 2)"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED (SELECT VALUE n FROM [{n:1}] START (CREATE ONLY t:9).n);
