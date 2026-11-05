/**
[test]
run = false
*/

-- Fixture for `mutate_perm_delete_denied.gql`: every record user may select
-- `person`, and no one may delete one.
DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL, FOR delete NONE;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:1 SET name = 'alice' RETURN NONE;
CREATE person:2 SET name = 'bob' RETURN NONE;
