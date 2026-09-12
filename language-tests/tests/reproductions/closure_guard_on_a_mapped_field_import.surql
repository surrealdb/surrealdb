/**
[test]

[[test.results]]
value = "'OK'"
*/
-- Fixture: a mapped field guarded by a closure invocation, where the record
-- and one of its array elements disagree on the flag the closure reads.
{
	DEFINE TABLE t PERMISSIONS FOR select FULL;
	DEFINE FIELD allowed ON t TYPE bool;
	DEFINE FIELD items ON t TYPE array<object>;
	DEFINE FIELD items[*].shown ON t TYPE number;
	DEFINE FIELD items[*].allowed ON t TYPE option<bool>;
	DEFINE FIELD items[*].secret ON t TYPE option<string> PERMISSIONS FOR select WHERE $pred();
	-- The record refuses. The element claims otherwise.
	CREATE t:1 SET allowed = false, items = [
		{ shown: 1, allowed: true, secret: 'LEAK' }
	];
	-- The same shape with the record allowing, to show the guard is live.
	CREATE t:2 SET allowed = true, items = [
		{ shown: 2, allowed: false, secret: 'SHOWN' }
	];
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
