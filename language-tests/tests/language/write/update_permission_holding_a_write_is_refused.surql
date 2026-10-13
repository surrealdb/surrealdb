/**
[env]
namespace = true
database = true

[test]
reason = """
Register #28, row PermissionOrAssertFrame: a `PERMISSIONS FOR update` predicate
that writes is refused where it is defined, on a normal table and on a relation
table alike, so no update-shaped statement can judge such a predicate. The
request fails as a whole at the first definition that holds the write.
"""

[test.results]
request-error = "Invalid statement: a write is not allowed in a permission (statement 3)"
*/
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE counter SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE thing SCHEMALESS
	PERMISSIONS FOR select, create FULL,
	FOR update WHERE (UPSERT ONLY counter:1 SET n += 1).n >= 0;

DEFINE TABLE edge TYPE RELATION IN person OUT person SCHEMALESS
	PERMISSIONS FOR select, create FULL,
	FOR update WHERE (UPSERT ONLY counter:1 SET n += 1).n >= 0;
