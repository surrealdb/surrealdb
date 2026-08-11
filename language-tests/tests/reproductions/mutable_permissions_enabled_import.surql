/**
[test]
reason = "Setup for mutable_permissions_enabled: a table whose create/update permission predicates write (through a function) to an audit table, plus a record user to trigger them. Defines successfully because the mutable_permissions capability is on."
run = false
*/

-- Idempotent markers so the assertions do not depend on how many times a
-- permission predicate is evaluated per operation.
DEFINE TABLE audit SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FUNCTION fn::mark_create() { UPSERT audit:created SET hit = true; RETURN true; };
DEFINE FUNCTION fn::mark_update() { UPSERT audit:updated SET hit = true; RETURN true; };

DEFINE TABLE thing SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR create WHERE fn::mark_create() OR true
		FOR update WHERE fn::mark_update() OR true;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:test;
