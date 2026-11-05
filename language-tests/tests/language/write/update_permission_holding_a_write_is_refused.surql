/**
[env]
namespace = true
database = true

[test]
reason = """
Register #28, row PermissionOrAssertFrame: a `PERMISSIONS FOR update` predicate
that writes is refused where it is defined, on a normal table and on a relation
table alike, so no update-shaped statement can judge such a predicate. Each
definition holding the write fails on its own.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `UPSERT` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `UPSERT` writes data"
*/
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE counter SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE thing SCHEMALESS
	PERMISSIONS FOR select, create FULL,
	FOR update WHERE (UPSERT ONLY counter:1 SET n += 1).n >= 0;

DEFINE TABLE edge TYPE RELATION IN person OUT person SCHEMALESS
	PERMISSIONS FOR select, create FULL,
	FOR update WHERE (UPSERT ONLY counter:1 SET n += 1).n >= 0;
