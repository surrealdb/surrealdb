/**
[test]
reason = """
	Setup for a_late_bound_read_applies_the_tables_permissions: a table whose
	select permission, computed fields and field permissions a record user's
	reads must apply, one field guard reading a computed field. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:probe;

DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE a > 0 FOR create, update FULL;
DEFINE FIELD c ON t COMPUTED a + 1;
DEFINE FIELD pos ON t COMPUTED a > 0 PERMISSIONS FOR select NONE;
DEFINE FIELD s ON t PERMISSIONS FOR select WHERE pos = true;
DEFINE FIELD h ON t PERMISSIONS FOR select NONE FOR update NONE;
CREATE t:1 SET a = 1, s = 'x', h = 'hidden';
CREATE t:2 SET a = -1, s = 'y';

DEFINE TABLE u SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD k ON u COMPUTED 'k';
DEFINE FIELD r ON u PERMISSIONS FOR select WHERE k = 'no';
CREATE u:1 SET r = 1;
