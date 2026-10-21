/**
[test]
reason = "Setup for vector_measure_param_permission.surql: a catalog parameter whose PERMISSIONS predicate reads the row it is read for, two tables of vectors, one whose every row admits the read and one with a row that refuses it, and the record user the test signs in as. Runs as owner."

[[test.results]]
value = "'OK'"

*/
{
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR TOKEN 15m, FOR SESSION 6h;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE PARAM $q VALUE [0f, 0f] PERMISSIONS WHERE open = true;
	DEFINE TABLE opened SCHEMALESS PERMISSIONS FOR select FULL;
	INSERT INTO opened [
		{ id: opened:1, v: [3f, 4f], open: true },
		{ id: opened:2, v: [1f, 0f], open: true }
	];
	DEFINE TABLE mixed SCHEMALESS PERMISSIONS FOR select FULL;
	INSERT INTO mixed [
		{ id: mixed:1, v: [3f, 4f], open: true },
		{ id: mixed:2, v: [1f, 0f], open: false }
	];
	RETURN 'OK';
};
