/**
[test]
reason = """
	Setup for write_keeps_guarded_fields: a table whose rows a record user
	may read only while a field that user may not select still holds its
	stored value, and the record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:reader;

DEFINE TABLE doc PERMISSIONS FOR select WHERE hidden = 'secret', FOR create, update FULL;
DEFINE FIELD hidden ON doc PERMISSIONS FOR select NONE;
CREATE doc:1 SET n = 1, hidden = 'secret';
CREATE doc:2 SET n = 2, hidden = 'secret';
