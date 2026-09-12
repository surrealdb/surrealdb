/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: closed:1, x: 1 }]"

[[test.results]]
value = "NONE"
*/
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNUP ( CREATE user SET email = $email )
	SIGNIN ( SELECT * FROM user WHERE email = $email )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE closed SCHEMALESS PERMISSIONS NONE;
CREATE closed:1 SET x = 1;
DEFINE FUNCTION fn::ids() { RETURN array::shuffle([closed:1, closed:gone]) };
