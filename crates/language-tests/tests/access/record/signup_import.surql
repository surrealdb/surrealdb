/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: foo:bar, published: true, user: user:other }]"

[[test.results]]
value = "[{ id: foo:baz, published: false, user: user:other }]"

[[test.results]]
value = "[{ id: foo:baq, published: false, user: user:bla }]"

[[test.results]]
value = "[{ id: foo:baq, published: false, user: user:bla }, { id: foo:bar, published: true, user: user:other }, { id: foo:baz, published: false, user: user:other }]"

*/
DEFINE ACCESS account ON DATABASE TYPE RECORD
	SIGNUP ({
		if(!string::is::email($email)){
			throw "Email field is not a valid email";
		};
		// id field to ensure consistant user ids so we can specify data upfront
		(CREATE user SET id = $id, email = $email, pass = crypto::argon2::generate($pass)).id
	})
	SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
	DURATION FOR TOKEN 17m, FOR SESSION 12h;

DEFINE TABLE foo
	PERMISSIONS
		FOR select
			WHERE published = true
			OR user = $auth;

CREATE foo:bar SET published = true, user = user:other;
CREATE foo:baz SET published = false, user = user:other;
CREATE foo:baq SET published = false, user = user:bla;
SELECT * FROM foo;
