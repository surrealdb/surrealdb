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

*/
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNUP ({
		if(!string::is::email($email)){
			throw "Email field is not a valid email";
		};
		(CREATE user SET email = $email, pass = crypto::argon2::generate($pass)).id
	})
	SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
	DURATION FOR TOKEN 16m, FOR SESSION 12h;


DEFINE TABLE OVERWRITE something SCHEMAFULL
    PERMISSIONS
        FOR CREATE, UPDATE
            WHERE $auth = user
        FOR SELECT, UPDATE, DELETE NONE;

DEFINE FIELD OVERWRITE user ON something
    TYPE record<user>
    DEFAULT $auth;

DEFINE FIELD OVERWRITE time ON something
    TYPE datetime
    VALUE time::now();
