/**
[test]
run = false
reason = "Setup only: a right table indexed on its key that hides every third row behind a table permission and carries a computed field, for join_lookup_permissions_record.pgsql and join_lookup_permissions_owner.pgsql."
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice' RETURN NONE;

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE a SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE p SCHEMALESS PERMISSIONS FOR select WHERE keep = true;
DEFINE FIELD up ON p COMPUTED string::uppercase(tag);
DEFINE INDEX p_j ON p FIELDS j;

FOR $i IN 0..120 { CREATE type::record('a', $i) SET n = $i, j = $i RETURN NONE; };
FOR $i IN 0..150 { CREATE type::record('p', $i) SET j = $i, tag = 't' + <string> $i, keep = ($i % 3 != 0) RETURN NONE; };
