/**
[test]
reason = "Setup for change_alter_permissions.surql: a record access method, a SCHEMAFULL table whose rows were stored before a migration, and a PERMISSIONS-only ALTER FIELD over them."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM type::record('person', $id)) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob RETURN NONE;
DEFINE TABLE doc SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD n ON doc TYPE string;
DEFINE FIELD s ON doc TYPE string;
CREATE doc:1 SET n = '3', s = 'a' RETURN NONE;
CREATE doc:2 SET n = '7', s = 'b' RETURN NONE;
ALTER FIELD n ON doc TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
ALTER FIELD n ON doc PERMISSIONS FOR select WHERE $value > 5;
ALTER FIELD s ON doc PERMISSIONS FOR select NONE;
