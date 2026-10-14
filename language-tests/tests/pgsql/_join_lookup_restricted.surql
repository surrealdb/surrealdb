/**
[test]
run = false
reason = "Setup only: a right table whose indexed key column a record user may not read, beside an indexed column it may, for join_lookup_restricted_record.pgsql and join_lookup_restricted_owner.pgsql."
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice' RETURN NONE;

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE a SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE b SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD k ON b PERMISSIONS FOR select NONE;
DEFINE INDEX b_k ON b FIELDS k;
DEFINE INDEX b_j ON b FIELDS j;

CREATE a:1 SET n = 1, k = 1, j = 1 RETURN NONE;
CREATE a:2 SET n = 2, k = 2, j = 2 RETURN NONE;
CREATE a:3 SET n = 3, k = 3, j = 9 RETURN NONE;

CREATE b:1 SET k = 1, j = 1, tag = 'x' RETURN NONE;
CREATE b:2 SET k = 2, j = 2, tag = 'y' RETURN NONE;
CREATE b:3 SET k = 3, j = 3, tag = 'z' RETURN NONE;
