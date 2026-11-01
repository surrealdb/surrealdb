/**
[test]
run = false
reason = "Setup for omitted_column_is_null and omitted_column_record_session: a schemafull table declared as CREATE TABLE declares one, with columns that hold NULL, one with a default, and the permissions a record user needs to write and read it"
*/

DEFINE TABLE users SCHEMALESS;
CREATE users:alice SET name = 'alice' RETURN NONE;

DEFINE ACCESS test ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('users', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE omit_t SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD a ON omit_t TYPE none | null | int;
DEFINE FIELD b ON omit_t TYPE none | null | string;
DEFINE FIELD d ON omit_t TYPE none | null | int DEFAULT 7;
