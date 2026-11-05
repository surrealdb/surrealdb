/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for insert_select_permission_skip_keeps_writes.surql: tables the
-- record user may write but not read, whose events write to tables it may
-- read, and the record user. Runs as owner.
{
	DEFINE TABLE seen SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE hidden SCHEMALESS PERMISSIONS FOR create, update FULL, FOR select NONE;
	DEFINE EVENT log ON hidden THEN { CREATE seen SET at = $event RETURN NONE };
	DEFINE TABLE strict_seen SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE strict SCHEMAFULL PERMISSIONS FOR create, update FULL, FOR select NONE;
	DEFINE FIELD req ON strict TYPE int;
	DEFINE FIELD v ON strict TYPE int;
	DEFINE EVENT log ON strict THEN { CREATE strict_seen SET at = $event, v = $after.v RETURN NONE };
	CREATE strict:1 SET req = 10, v = 1;
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
