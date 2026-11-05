/**
[env]
namespace = true
database = true
auth = { level = "owner" }
# The engines the harness opens without versioning, which refuse the read with an error of
# their own. A versioned store answers it, and an engine the harness cannot name may be either.
backend = ["mem", "surrealkv", "tikv"]

[test]
reason = """
	A versioned read locked for update in a subquery is answered by the store, here with its
	refusal of a versioned read, rather than declined by the planner."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
error = "Internal database error: reading the definition of table `person`: The underlying datastore does not support versioned queries"
*/
DEFINE TABLE person SCHEMALESS;
CREATE person:1;
RETURN (SELECT * FROM person:1 VERSION d'2024-01-01T00:00:00Z' FOR UPDATE);
