/**
[test]
run = false
reason = "Setup for declared_columns_record_session and declared_columns_viewer_session: a record user, and a schemafull table everyone may read whose columns a record user has no right to be told of"
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice' RETURN NONE;

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE declcol_acct SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD name ON declcol_acct TYPE string;
DEFINE FIELD hidden ON declcol_acct TYPE option<string> PERMISSIONS NONE;
DEFINE FIELD nick ON declcol_acct TYPE option<string>;

CREATE declcol_acct:1 SET name = 'Ann', hidden = 'h', nick = 'A' RETURN NONE;
CREATE declcol_acct:2 SET name = 'Ben' RETURN NONE;
