/**
[env.capabilities]
deny-arbitrary-query = ["system"]

[test]
reason = """
	surrealdb.query() in a script runs arbitrary SurrealQL, so it must pass the
	same arbitrary-query subject gate the request front door applies
	(GHSA-fq69-wr4w-v3gm). With arbitrary queries denied for the acting subject,
	a script must not be able to run one through the bridge. The unfixed bridge
	ran the nested query with no capability check, so this returned a value.
"""

[[test.results]]
error = true
*/

RETURN function() { return await surrealdb.query("RETURN 1"); };
