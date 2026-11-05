/**
[test]
reason = "Setup for 1718_a_mixed_write_stores_the_record_not_the_callers_view.surql."
run = false
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:u;
DEFINE TABLE rr SCHEMALESS PERMISSIONS
	FOR select, update WHERE open = true
	FOR create, delete NONE;
DEFINE FIELD secret ON rr PERMISSIONS FOR select NONE;
-- Each update of `rr` records the stored record's `secret` where the record
-- user can read it.
DEFINE TABLE seen SCHEMALESS PERMISSIONS FULL;
DEFINE EVENT seen ON rr WHEN $event = 'UPDATE' THEN {
	CREATE seen SET of = $after.id, secret = $after.secret
};
CREATE rr:[1] SET open = true, secret = 's1';
CREATE rr:[5] SET open = true, secret = 's5';
CREATE rr:[6] SET open = false, secret = 's6';
