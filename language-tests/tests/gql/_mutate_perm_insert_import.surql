/**
[test]
run = false
*/

-- Fixture for `mutate_perm_insert_denied.gql`: every record user may select
-- `person`, and no one may create one.
DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL, FOR create NONE;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:1 SET name = 'alice' RETURN NONE;
