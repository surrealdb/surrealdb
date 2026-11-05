/**
[test]
reason = "Setup for bruteforce_knn_field_permission.surql: a table whose vector field a field-level SELECT permission hides from other owners, a table whose vector is COMPUTED with one record that stored bytes under the field's name before the field was defined, and the record user the test signs in as. Runs as owner."

[[test.results]]
value = "'OK'"

*/
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE TABLE fp SCHEMALESS PERMISSIONS FOR select FULL;
	DEFINE FIELD point ON fp PERMISSIONS FOR select WHERE owner = $auth;
	INSERT INTO fp [
		{ id: fp:1, point: [1f], owner: user:alice },
		{ id: fp:2, point: [0f], owner: user:bob },
		{ id: fp:3, point: [2f], owner: user:alice }
	];
	DEFINE TABLE cp SCHEMALESS PERMISSIONS FOR select FULL;
	INSERT INTO cp [{ id: cp:1, x: 3, point: [0f] }];
	DEFINE FIELD point ON cp COMPUTED [x * 1f];
	INSERT INTO cp [
		{ id: cp:2, x: 1 },
		{ id: cp:3, x: 2 }
	];
	RETURN 'OK';
};
