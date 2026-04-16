/**
[test]
run = false
*/

DEFINE ACCESS user_access ON DATABASE TYPE RECORD
	SIGNUP ( CREATE user SET pass = crypto::argon2::generate($pass) )
	SIGNIN ( SELECT * FROM user WHERE crypto::argon2::compare(pass, $pass) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE a SCHEMAFULL PERMISSIONS FULL;

DEFINE TABLE b SCHEMAFULL PERMISSIONS
	FOR create WHERE false
	FOR select FULL;

DEFINE FIELD a ON b TYPE record<a> REFERENCE;

CREATE a:test;
