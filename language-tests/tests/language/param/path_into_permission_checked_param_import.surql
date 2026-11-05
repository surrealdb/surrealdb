/**
[test]
reason = "Setup for path_into_permission_checked_param: a record access method and DEFINE PARAMs whose PERMISSIONS deny, admit, and conditionally deny a record user."

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
	SIGNUP ( CREATE user SET email = $email )
	SIGNIN ( SELECT * FROM user WHERE email = $email )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE PARAM $secret VALUE { a: [1, 2] } PERMISSIONS NONE;
DEFINE PARAM $open VALUE { a: [3, 4] } PERMISSIONS FULL;
DEFINE PARAM $gated VALUE { a: [5, 6] } PERMISSIONS WHERE $auth.email = 'someone-else@example.com';
