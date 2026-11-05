/**
[env]
namespace = true
database = true

[test]
reason = """
Register #28, row PermissionOrAssertFrame: a `PERMISSIONS FOR create`, `update`
or `delete` predicate may not write either. The permission is refused at the
statement that defines it, not at the first read or write that judges it, and
that statement alone fails.

The two clauses below differ only in which action they guard, and both are
refused: the row names the permission frame, not the action it guards. The
`audit` definition before them holds no write and succeeds.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `CREATE` writes data"
*/
DEFINE TABLE audit SCHEMALESS PERMISSIONS FULL;

-- A guard on what the caller may write.
DEFINE TABLE doc SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR create, delete FULL
		FOR update WHERE (CREATE audit SET saw = id) != NONE;

-- A guard on what the caller may read.
DEFINE TABLE watched SCHEMALESS
	PERMISSIONS
		FOR select WHERE (CREATE audit SET saw = id) != NONE
		FOR create, update, delete FULL;
