/**
[test]
reason = """
	Setup for a_guard_derives_a_field_that_reads_a_denied_table: a table
	permission reading a computed field whose body reads, under a LIMIT, a
	table record users may not select. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:probe;

DEFINE TABLE flag SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE flagged != true, FOR create, update FULL;
DEFINE FIELD flagged ON doc COMPUTED (SELECT VALUE id FROM ONLY flag WHERE target = $parent.id LIMIT 1) != NONE;
INSERT INTO doc [{ id: doc:pub, status: 'public' }, { id: doc:sec, status: 'secret' }];
CREATE flag:1 SET target = doc:sec;
DEFINE TABLE holder SCHEMALESS PERMISSIONS FULL;
CREATE holder:1 SET d = doc:sec, e = doc:pub;
