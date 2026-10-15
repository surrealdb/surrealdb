/**
[test]
reason = "A table with rows behind a migration step, changed from SCHEMAFULL to SCHEMALESS and back: every change is accepted, every read returns the same values on either side, the steps and their generations are kept, a table that is no longer enforced accepts what an enforced one refuses, one that is enforced again is held to the rules and verifies the fields that are not guaranteed, and one made SCHEMALESS by redefining it without its TYPE is refused while a row is behind a step."

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
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "{ generation: 5, guaranteed: [], migrations: [{ field: 'a', generation: 4, name: 'a_int' }], progress: { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }"

[[test.results]]
error = "Cannot change field `b` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "{ generation: 6, guaranteed: [], migrations: [{ field: 'a', generation: 4, name: 'a_int' }], progress: { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 9, b: 3, extra: 'e', id: tb:3 }]"

[[test.results]]
value = "[5, -1, 9]"

[[test.results]]
value = "[true, true, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['5', '-1', '9']"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }, { field: 'a', generation: 7, name: 'a_str' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '5', b: 'x', id: tb:1 }, { a: '-1', b: 'y', id: tb:2 }, { a: '9', b: 3, extra: 'e', id: tb:3 }]"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }, { field: 'a', generation: 7, name: 'a_str' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ failures: 0, field: 'a', finished: false }, { failures: 0, field: 'b', finished: false }]"

[[test.results]]
error = "Cannot change field `b` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `a` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, -1, 9]"

[[test.results]]
value = "10"

[[test.results]]
error = "Cannot migrate table `tb`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, -1, 9]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, -1, 9]"

[[test.results]]
value = "12"

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
value = "{ generation: 4, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

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
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "{ generation: 7, guaranteed: [], migrations: [{ field: 'a', generation: 4, name: 'a_int' }], progress: { fallbacks: 0, lap: 3, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD b ON tb TYPE string;
CREATE tb:1 SET a = '5', b = 'x' RETURN NONE;
CREATE tb:2 SET a = 'junk', b = 'y' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- Enforced: the change of b is refused.
ALTER FIELD b ON tb TYPE int;
-- SCHEMAFULL to SCHEMALESS.
ALTER TABLE tb SCHEMALESS;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- No longer enforced: the change of b is accepted, and a row may hold what the table does not declare.
ALTER FIELD b ON tb TYPE int;
CREATE tb:3 SET a = 9, b = 3, extra = 'e' RETURN AFTER;
SELECT VALUE a FROM tb ORDER BY id;
SELECT VALUE type::is_int(a) FROM tb ORDER BY id;
-- A migration while SCHEMALESS mints the next generation.
ALTER FIELD a ON tb TYPE string MIGRATE EXISTING ROWS AS a_str USING <string>$value FALLBACK '';
SELECT VALUE a FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
(INFO FOR TABLE tb).migrations.guaranteed;
-- SCHEMALESS to SCHEMAFULL: rows are read as they were, the fields that are not guaranteed are verified.
ALTER TABLE tb SCHEMAFULL;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
(INFO FOR TABLE tb).migrations.guaranteed;
(INFO FOR TABLE tb).migrations.verification;
-- Enforced again: the change is refused as before.
ALTER FIELD b ON tb TYPE string;
ALTER FIELD a ON tb TYPE int;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int2 USING <int>$value FALLBACK -2;
SELECT VALUE a FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.generation;
-- Redefining the table without its TYPE changes it from NORMAL to ANY, which is refused while a row is behind a step.
DEFINE TABLE OVERWRITE tb SCHEMALESS;
DEFINE TABLE OVERWRITE tb TYPE NORMAL SCHEMALESS;
SELECT VALUE a FROM tb ORDER BY id;
DEFINE TABLE OVERWRITE tb TYPE NORMAL SCHEMAFULL;
SELECT VALUE a FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.generation;
-- A table with no rows queues no verification either way.
DEFINE TABLE em SCHEMAFULL;
DEFINE FIELD a ON em TYPE string;
ALTER TABLE em SCHEMALESS;
ALTER FIELD a ON em TYPE int;
ALTER TABLE em SCHEMAFULL;
(INFO FOR TABLE em).migrations;
-- The same at compatibility level 3: neither direction enforces or queues a verification; becoming SCHEMAFULL starts the pass the migration queued over.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD b ON tb TYPE string;
CREATE tb:1 SET a = '5', b = 'x' RETURN NONE;
CREATE tb:2 SET a = 'junk', b = 'y' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER TABLE tb SCHEMALESS;
SELECT * FROM tb ORDER BY id;
ALTER TABLE tb SCHEMAFULL;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
ALTER FIELD b ON tb TYPE int;
(INFO FOR TABLE tb).migrations.migrations;
DEFINE TABLE OVERWRITE tb SCHEMALESS;
SELECT * FROM tb ORDER BY id;
