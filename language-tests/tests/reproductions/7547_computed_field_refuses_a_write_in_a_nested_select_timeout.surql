/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A computed field refuses a write in the `TIMEOUT` clause of a nested `SELECT`
	as it refuses one anywhere else in its body. The `DEFINE FIELD` follows another
	statement of the request, so it is planned when it is reached and fails on
	its own with the clause-effect error; it does not refuse the request.
"""
issue = 7547

[[test.results]]
value = "NONE"

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED (SELECT VALUE n FROM [{n:1}] TIMEOUT (CREATE ONLY t:9).d);
