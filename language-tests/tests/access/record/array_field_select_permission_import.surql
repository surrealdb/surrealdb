/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for array_field_select_permission.surql: a table carrying an array of
-- objects, one of whose keys the record user may not select, alongside a denied
-- top-level field that no array element defines. Runs as owner.
{
	DEFINE TABLE t PERMISSIONS FOR select, create, update FULL;
	DEFINE FIELD items ON t TYPE array<object>;
	DEFINE FIELD items[*].shown ON t TYPE number;
	DEFINE FIELD items[*].denied ON t TYPE option<string> PERMISSIONS FOR select NONE;
	-- Denied, and named after nothing inside `items`.
	DEFINE FIELD elsewhere ON t TYPE option<string> PERMISSIONS FOR select NONE;
	CREATE t:1 SET
		elsewhere = 'ELSEWHERE_SECRET',
		items = [
			{ shown: 1, denied: 'ITEM_SECRET_1' },
			{ shown: 2, denied: 'ITEM_SECRET_2' },
			{ shown: 3, denied: 'ITEM_SECRET_3' }
		];
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
