/**
[test]
reason = "Import: a table fully readable by record users whose `score` field carries a SELECT permission the signed-in owner satisfies on every row, plus an index over `owner`. Used to verify that withholding a restricted ORDER BY from access-path selection does not also suppress the outer LIMIT/START."
run = false
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice';

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Returns the table name so `SELECT ... FROM type::table(fn::tb())` is planned
-- through the runtime-resolved DynamicScan fallback instead of the plan-time
-- access path.
DEFINE FUNCTION fn::tb() { RETURN 'doc'; } PERMISSIONS FULL;

DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON doc TYPE record<users>;
DEFINE FIELD score ON doc TYPE int PERMISSIONS FOR select WHERE owner = $auth.id;
-- Indexing `owner` (not `score`) gives the B-tree access path a reason to be
-- chosen while ORDER BY still targets the restricted field.
DEFINE INDEX doc_owner ON doc FIELDS owner;

-- Every row belongs to alice, so the permission is satisfied on all of them and
-- the whole sort key is visible to her. Value order (10 < 20 < 30 < 40)
-- deliberately differs from record-id order (doc:1 < doc:2 < doc:3 < doc:4).
CREATE doc:1 SET owner = users:alice, score = 10;
CREATE doc:2 SET owner = users:alice, score = 40;
CREATE doc:3 SET owner = users:alice, score = 20;
CREATE doc:4 SET owner = users:alice, score = 30;
