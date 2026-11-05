/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"

[test]
reason = "A level-4 table made SCHEMAFULL over rows, and a database raised to level 4, queue a check for rows holding a top-level key no definition declares, beside the verification of each field, since a write of such a row is refused. INFO shows the queued check as `undeclared`; the job that runs it never runs here. A table with no rows, an import and level 3 queue none, and REMOVE UNDECLARED FIELDS clears the key the check would count."

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
value = "{ generation: 2, guaranteed: [], migrations: [], undeclared: { failures: 0, finished: false }, verification: [{ failures: 0, field: 'a', finished: false }] }"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 2, id: t:2, z: 'x' }]"

[[test.results]]
error = "Found field 'z', but no such field exists for table 't'"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'lvl3'"

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
error = "Found field 'z', but no such field exists for table 't'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ failures: 0, finished: false }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 2, id: t:1 }]"

[[test.results]]
value = "{ failures: 0, finished: false }"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE int;
CREATE t:1 SET a = 1 RETURN NONE;
CREATE t:2 SET a = 2, z = 'x' RETURN NONE;
ALTER TABLE t SCHEMAFULL;
(INFO FOR TABLE t).migrations;
-- The row keeps the key, and a write of it is refused.
SELECT * FROM t ORDER BY id;
UPDATE t:2 SET a = 3 RETURN NONE;
UPDATE t:1 SET a = 3 RETURN NONE;
-- A table with no rows queues none.
DEFINE TABLE em SCHEMALESS;
ALTER TABLE em SCHEMAFULL;
(INFO FOR TABLE em).migrations.undeclared;
-- An import restores definitions as they were, and queues none.
DEFINE TABLE imp SCHEMALESS;
CREATE imp:1 SET z = 1 RETURN NONE;
OPTION IMPORT;
ALTER TABLE imp SCHEMAFULL;
OPTION IMPORT = false;
(INFO FOR TABLE imp).migrations.undeclared;
-- Level 3 refuses the write too, and queues no check; raising the database to level 4 does.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE int;
CREATE t:1 SET a = 1, z = 'x' RETURN NONE;
ALTER TABLE t SCHEMAFULL;
(INFO FOR TABLE t).migrations;
UPDATE t:1 SET a = 2 RETURN NONE;
ALTER DATABASE lvl3 COMPATIBILITY 4;
(INFO FOR TABLE t).migrations.undeclared;
-- REMOVE UNDECLARED FIELDS clears the key from every row stored before it, so the row is written.
ALTER TABLE t MIGRATE EXISTING ROWS AS tidy REMOVE UNDECLARED FIELDS;
UPDATE t:1 SET a = 2 RETURN NONE;
SELECT * FROM t;
(INFO FOR TABLE t).migrations.undeclared;
