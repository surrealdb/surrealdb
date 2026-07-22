/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNUP ( CREATE user SET email = $email, admin = $admin )
	SIGNIN ( SELECT * FROM user WHERE email = $email )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE PARAM $secret VALUE 42 PERMISSIONS WHERE $auth.admin = true;
DEFINE FUNCTION fn::secret() { RETURN 1 } PERMISSIONS WHERE $auth.admin = true;
DEFINE PARAM $open VALUE 7 PERMISSIONS WHERE $auth != NONE;
