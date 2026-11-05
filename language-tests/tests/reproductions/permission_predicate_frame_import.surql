/**
[test]
reason = """
	Setup for permission_predicate_frame: a function and a parameter
	declared `PERMISSIONS NONE`, each read only from inside a table's
	`PERMISSIONS FOR select` predicate, and the record user the test runs
	as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE FUNCTION fn::is_open() { RETURN true; } PERMISSIONS NONE;
DEFINE PARAM $open VALUE true PERMISSIONS NONE;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE fn::is_open();
DEFINE TABLE doc2 SCHEMALESS PERMISSIONS FOR select WHERE $open = true;
CREATE doc:1;
CREATE doc2:1;
