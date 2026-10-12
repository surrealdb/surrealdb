/**
[env]
namespace = true
database = true

[test]
reason = """
	The `id` definition's own clauses are held to the rule of every other
	clause: an ASSERT that calls a function evaluated against the record may
	read any component, so a change that drops or renames a name is refused,
	naming the clause. A change that only moves the names keeps every one of
	them, and is accepted.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `t`: the ASSERT of the field `id` reads fields of the record by names that cannot be determined, so it may read its component `n`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD id ON u TYPE [n: int, a: int] ASSERT fn::positive() PERMISSIONS FULL'"
*/

DEFINE FUNCTION fn::positive() { RETURN n > 0; };
DEFINE FIELD id ON t TYPE [a: int, n: int] ASSERT fn::positive();
ALTER FIELD id ON t TYPE [a: int, m: int];
REMOVE FIELD id ON t;

-- Moving the names keeps them all.
DEFINE FIELD id ON u TYPE [a: int, n: int] ASSERT fn::positive();
ALTER FIELD id ON u TYPE [n: int, a: int];
(INFO FOR TABLE u).fields.id;
