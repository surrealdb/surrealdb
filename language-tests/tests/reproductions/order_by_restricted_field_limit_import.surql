/**
[test]
reason = "Import: a table a record user may select, carrying a field whose SELECT permission grants it to the row's owner. Every row belongs to the signed-in user, so the permission grants the sort key on every row and the key is fully visible. Value order (40 > 30 > 20 > 10) deliberately differs from record-id order (doc:1 < doc:2 < doc:3 < doc:4), and an index on `owner` gives the WHERE form a B-tree access path."
run = false
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice';

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE FUNCTION fn::tb() { RETURN 'doc'; } PERMISSIONS FULL;

DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON doc TYPE record<users>;
DEFINE FIELD score ON doc TYPE int PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE INDEX doc_owner ON doc FIELDS owner;

CREATE doc:1 SET owner = users:alice, score = 10;
CREATE doc:2 SET owner = users:alice, score = 40;
CREATE doc:3 SET owner = users:alice, score = 20;
CREATE doc:4 SET owner = users:alice, score = 30;
