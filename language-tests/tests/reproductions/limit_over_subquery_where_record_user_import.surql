/**
[test]
reason = "Setup for limit_over_subquery_where_record_user.surql and limit_over_subquery_where_record_user_shapes.surql. Runs as owner."

[[test.results]]
value = "'OK'"

*/
-- Fixture: tables whose select permission reads `$auth`'s record, so which rows
-- a caller sees depends on the caller, and a record user to read them as.
-- `u` and `ku` hold rows n = 0..39 with k = n % 5; a caller sees the even n
-- other than its own `c`, which is 4 for user:alice. `u` takes random record
-- ids, so its scan order is arbitrary, and `ku` takes n as its id, so it scans
-- in n order. `other` holds the same rows under a permission that reads
-- nothing of the caller.
{
	DEFINE TABLE u PERMISSIONS FOR select WHERE n != $auth.c AND n % 2 = 0;
	DEFINE TABLE ku PERMISSIONS FOR select WHERE n != $auth.c AND n % 2 = 0;
	DEFINE TABLE other PERMISSIONS FOR select FULL;
	FOR $i IN 0..40 {
		CREATE u SET n = $i, k = $i % 5;
		CREATE type::record('ku', $i) SET n = $i, k = $i % 5;
		CREATE other SET n = $i, k = $i % 5;
	};
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret'), c = 4;
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
