/**
[test]

[[test.results]]
value = "'OK'"

*/
-- Fixture for computed_field_conditional_permission.surql: computed fields whose
-- `PERMISSIONS FOR select` predicate reads another field — one the record user
-- may select, one it may not. Both predicates answer differently depending on
-- whether they are evaluated against the record or against the reduced view.
-- Runs as owner.
{
	DEFINE TABLE item PERMISSIONS FOR select, create, update FULL;
	DEFINE FIELD counter ON item TYPE option<number> PERMISSIONS FOR select, update FULL;
	DEFINE FIELD secret ON item VALUE 'STORED_SECRET' PERMISSIONS FOR select NONE;
	-- Gated on the denied stored field. `secret` is always set, so the predicate
	-- is false on the record and the field is denied — but it holds on a reduced
	-- view, where `secret` has already been cut.
	DEFINE FIELD gated_by_denied ON item COMPUTED 'GATED_SECRET'
		PERMISSIONS FOR select WHERE secret = NONE;
	-- Gated on a field the caller may select: allowed either way, the control.
	DEFINE FIELD gated_by_visible ON item COMPUTED 'GATED_OK'
		PERMISSIONS FOR select WHERE counter != NONE;
	DEFINE FIELD leak ON item TYPE any PERMISSIONS FOR select, update FULL;
	CREATE item:1 SET counter = 1;
	DEFINE TABLE user PERMISSIONS FULL;
	CREATE user:alice SET email = 'alice@surrealdb.com', pass = crypto::argon2::generate('secret');
	DEFINE ACCESS account ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1h;
	'OK'
};
