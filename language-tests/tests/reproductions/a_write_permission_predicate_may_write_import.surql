/**
[test]
reason = """
Import for a_write_permission_predicate_may_write.surql: one table whose
`PERMISSIONS FOR update` predicate writes while it judges, and one whose
`PERMISSIONS FOR select` predicate tries the same. The two clauses differ only
in which action they guard, so anything the test finds between them is the
frame each is judged in. Runs as owner.
"""
run = false
*/

DEFINE TABLE audit SCHEMALESS PERMISSIONS FULL;

-- A guard on what the caller may write is reached from a statement that is
-- already writing, so it may record the attempt.
DEFINE TABLE doc SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR create, delete FULL
		FOR update WHERE (CREATE audit SET saw = id) != NONE;

-- A guard on what the caller may read is not.
DEFINE TABLE watched SCHEMALESS
	PERMISSIONS
		FOR select WHERE (CREATE audit SET saw = id) != NONE
		FOR create, update, delete FULL;

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:1;
CREATE doc:1 SET n = 0;
CREATE watched:1 SET n = 0;
