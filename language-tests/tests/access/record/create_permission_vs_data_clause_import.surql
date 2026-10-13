/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for create_permission_vs_data_clause.surql: two tables whose create
-- permission differs only in kind (`NONE` vs conditional), a `log` table the data
-- clause writes into, and the record user the test signs in as. Runs as owner.
{
	DEFINE TABLE pnone PERMISSIONS FOR select FULL, FOR create NONE;
	DEFINE TABLE pcond PERMISSIONS FOR select FULL, FOR create WHERE false;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
