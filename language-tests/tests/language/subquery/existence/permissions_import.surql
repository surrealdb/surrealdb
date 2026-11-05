/**
[test]
reason = """
	Setup for permissions: tables a record user reads through their select permissions (one whose
	predicate raises on the second row, one that depends on the row, one that depends on the caller)
	and the record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
CREATE user:alice SET name = 'alice' RETURN NONE;
DEFINE TABLE outer SCHEMALESS PERMISSIONS FOR select FULL;
CREATE outer:1 SET x = 1 RETURN NONE;
CREATE outer:2 SET x = 2 RETURN NONE;
DEFINE FUNCTION fn::check($id: record) { IF $id = box:2 { THROW 'denied by function'; }; RETURN true; } PERMISSIONS FULL;
DEFINE TABLE box SCHEMALESS PERMISSIONS FOR select WHERE fn::check(id);
CREATE box:1 SET x = 1, public = true RETURN NONE;
CREATE box:2 SET x = 2, public = false RETURN NONE;
CREATE box:3 SET x = 2, public = true RETURN NONE;
DEFINE TABLE flagged SCHEMALESS PERMISSIONS FOR select WHERE public = true;
CREATE flagged:1 SET x = 9, public = false RETURN NONE;
CREATE flagged:2 SET x = 2, public = true RETURN NONE;
CREATE flagged:3 SET x = 1, public = true RETURN NONE;
DEFINE TABLE mine SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth.id;
CREATE mine:1 SET x = 9, owner = user:bob RETURN NONE;
CREATE mine:2 SET x = 2, owner = user:alice RETURN NONE;
CREATE mine:3 SET x = 1, owner = user:alice RETURN NONE;
DEFINE TABLE hidden SCHEMALESS PERMISSIONS FOR select NONE;
CREATE hidden:1 SET x = 1 RETURN NONE;
