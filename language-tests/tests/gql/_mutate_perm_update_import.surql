/**
[test]
run = false
*/

-- Fixture for `mutate_perm_update_denied.gql`: every record user may select
-- `person`, and may update only its own record.
DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL, FOR update WHERE id = $auth.id;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:1 SET name = 'alice', age = 10 RETURN NONE;
CREATE person:2 SET name = 'bob', age = 20 RETURN NONE;
CREATE person:3 SET name = 'carol', age = 30 RETURN NONE;
