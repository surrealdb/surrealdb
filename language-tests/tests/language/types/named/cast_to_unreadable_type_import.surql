/**
[test]
reason = "Setup for cast_to_unreadable_type_is_refused: a hidden and a public type, and a record user"
run = false
*/
DEFINE TYPE hidden AS 'a' | 'b' PERMISSIONS NONE;
DEFINE TYPE shown AS 'a' | 'b';
DEFINE TABLE user PERMISSIONS FULL;
CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
	DURATION FOR SESSION 1h;
