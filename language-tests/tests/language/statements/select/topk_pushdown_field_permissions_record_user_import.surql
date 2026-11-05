/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for topk_pushdown_field_permissions_record_user.surql: an ORDER BY
-- field whose select permission admits it on some rows and cuts it on others,
-- and a record user to read it as. Runs as owner.
{
	DEFINE TABLE p SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE FIELD pub ON p TYPE int;
	DEFINE FIELD secret ON p TYPE int PERMISSIONS FOR select WHERE pub > 1;
	CREATE p:1 SET secret = 3, pub = 1;
	CREATE p:2 SET secret = 1, pub = 2;
	CREATE p:3 SET secret = 2, pub = 3;
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
