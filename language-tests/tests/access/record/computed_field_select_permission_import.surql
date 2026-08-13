/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for computed_field_select_permission.surql: a table whose COMPUTED
-- fields are denied to the record user by their own `PERMISSIONS FOR select`,
-- one that is allowed, a stored field denied the same way, and a writable field
-- that a statement could copy any of them into. Runs as owner.
{
	DEFINE TABLE item PERMISSIONS FOR select, create, update FULL;
	-- Denied outright.
	DEFINE FIELD hidden ON item COMPUTED 'COMPUTED_SECRET' PERMISSIONS FOR select NONE;
	-- Denied by a predicate this session does not satisfy.
	DEFINE FIELD conditional ON item COMPUTED 'CONDITIONAL_SECRET'
		PERMISSIONS FOR select WHERE $auth.admin = true;
	-- Denied, and derived from a stored field rather than a constant.
	DEFINE FIELD derived ON item COMPUTED counter * 2 PERMISSIONS FOR select NONE;
	-- Allowed: the control for every case below.
	DEFINE FIELD visible ON item COMPUTED 'VISIBLE' PERMISSIONS FOR select FULL;
	-- A stored field denied the same way, filtered by the reduce kernel.
	DEFINE FIELD secret ON item VALUE 'STORED_SECRET' PERMISSIONS FOR select NONE;
	DEFINE FIELD counter ON item TYPE option<number> PERMISSIONS FOR select, update FULL;
	DEFINE FIELD leak ON item TYPE any PERMISSIONS FOR select, update FULL;
	CREATE item:1 SET counter = 1;
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
