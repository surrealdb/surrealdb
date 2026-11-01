/**
[test]
run = false
reason = "Setup only: a table every user reads and a table with a row a record user cannot read, with the record access method that signs the user in, read by intersect_except_permissions"
*/

DEFINE TABLE ie_user SCHEMALESS;
CREATE ie_user:alice SET name = 'alice' RETURN NONE;

DEFINE ACCESS ie_access ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('ie_user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE ie_member SCHEMALESS PERMISSIONS FULL;
CREATE ie_member:1 SET g = 1 RETURN NONE;
CREATE ie_member:2 SET g = 2 RETURN NONE;
CREATE ie_member:3 SET g = 3 RETURN NONE;
CREATE ie_member:4 SET g = 3 RETURN NONE;

DEFINE TABLE ie_group SCHEMALESS PERMISSIONS FOR select WHERE open = true;
CREATE ie_group:1 SET gid = 1, open = true RETURN NONE;
CREATE ie_group:2 SET gid = 2, open = true RETURN NONE;
CREATE ie_group:3 SET gid = 3, open = false RETURN NONE;
