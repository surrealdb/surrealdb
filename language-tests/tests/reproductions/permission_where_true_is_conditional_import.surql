/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for permission_where_true_is_conditional.surql: a table whose
-- select permission is the conditional clause `WHERE true`, a string field the
-- test's predicate misuses, and a record user to read it as. Runs as owner.
{
	DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE true;
	DEFINE FIELD secret ON t TYPE string;
	CREATE t:1 SET secret = 'hidden';
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
