/**
[test]
reason = "Import: schema and seed data for table PERMISSIONS $before/$after tests"
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $auth.id );

DEFINE TABLE item SCHEMAFULL PERMISSIONS
	FOR select WHERE deleted IS NONE
	FOR create WHERE $after.name != NONE
	FOR update WHERE $before.deleted IS NONE
	FOR delete WHERE NONE;
DEFINE FIELD name ON item TYPE option<string>;
DEFINE FIELD deleted ON item TYPE option<datetime>;

CREATE user:test;
CREATE item:one SET name = 'one';
