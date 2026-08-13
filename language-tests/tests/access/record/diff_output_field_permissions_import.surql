/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for diff_output_field_permissions.surql: a table whose stored and
-- computed fields differ in what the record user may select, all of them
-- derived from or written alongside one visible field so a single write moves
-- every side at once. `value` is named after a JSON Patch op key on purpose.
-- Runs as owner.
{
	DEFINE TABLE t PERMISSIONS FOR select, create, update FULL;
	DEFINE FIELD counter ON t TYPE option<number> PERMISSIONS FOR select, update FULL;
	DEFINE FIELD secret ON t TYPE option<string> PERMISSIONS FOR select NONE, FOR update FULL;
	DEFINE FIELD hidden ON t COMPUTED counter * 2 PERMISSIONS FOR select NONE;
	DEFINE FIELD shown ON t COMPUTED counter * 3 PERMISSIONS FOR select FULL;
	DEFINE FIELD secret_set ON t COMPUTED secret != NONE PERMISSIONS FOR select FULL;
	DEFINE FIELD value ON t TYPE option<string> PERMISSIONS FOR select NONE, FOR update FULL;
	CREATE t:1 SET counter = 1;
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
