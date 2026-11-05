/**
[test]
reason = """
	Setup for permissions: a table whose select permission reads the row, a table with a field
	permission, a table with no restriction and a table nobody may read, and the record user the test
	runs as. Runs as owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
CREATE user:alice SET name = 'alice' RETURN NONE;
DEFINE TABLE owned SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
CREATE owned:1 SET owner = user:alice RETURN NONE;
CREATE owned:2 SET owner = user:bob RETURN NONE;
CREATE owned:3 SET owner = user:alice RETURN NONE;
CREATE owned:4 SET owner = user:bob RETURN NONE;
CREATE owned:5 SET owner = user:alice RETURN NONE;
CREATE owned:6 SET owner = user:alice RETURN NONE;
DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD secret ON guarded PERMISSIONS FOR select WHERE owner = $auth.id;
CREATE guarded:1 SET owner = user:alice, secret = 'a' RETURN NONE;
CREATE guarded:2 SET owner = user:bob, secret = 'b' RETURN NONE;
CREATE guarded:3 SET owner = user:alice, secret = 'c' RETURN NONE;
CREATE guarded:4 SET owner = user:bob, secret = 'd' RETURN NONE;
DEFINE TABLE open SCHEMALESS PERMISSIONS FOR select FULL;
CREATE open:1 RETURN NONE;
CREATE open:2 RETURN NONE;
CREATE open:3 RETURN NONE;
CREATE open:4 RETURN NONE;
CREATE open:5 RETURN NONE;
DEFINE TABLE denied SCHEMALESS PERMISSIONS FOR select NONE;
CREATE denied:1 RETURN NONE;
CREATE denied:2 RETURN NONE;
