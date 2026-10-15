/**
[test]
reason = "ALTER DATABASE COMPATIBILITY, either way, with rows behind migration steps: the level changes only which statements are accepted and which fields are verified, never what a read returns or which step a row replays. Raising it queues a verification and no rewrite, it is judged by the transaction that changes it, a cancelled change leaves the level alone, and the tables it does not enforce are left alone."

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
value = "4"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }]"

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
value = "[{ a: '5', b: 'x', id: tb:1 }, { a: '-1', b: 'y', id: tb:2 }]"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }, { field: 'a', generation: 8, name: 'a_str' }]"

[[test.results]]
value = "[{ failures: 0, field: 'b', finished: false }, { failures: 0, field: 'c', finished: false }, { failures: 0, field: 'a', finished: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "[{ failures: 0, field: 'b', finished: false }, { failures: 0, field: 'c', finished: false }, { failures: 0, field: 'a', finished: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ a: '5', b: 'x', id: tb:1 }, { a: '-1', b: 'y', id: tb:2 }]"

[[test.results]]
value = "9"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }, { field: 'a', generation: 8, name: 'a_str' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ failures: 0, field: 'a', finished: false }, { failures: 0, field: 'b', finished: false }, { failures: 0, field: 'c', finished: false }]"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ failures: 0, field: 'a', finished: false }]"

[[test.results]]
error = "Cannot change field `b` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `a` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, -1]"

[[test.results]]
value = "11"

[[test.results]]
value = "NONE"

[[test.results]]
value = "11"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot change field `b` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "4"

[[test.results]]
value = "'DEFINE FIELD b ON `tb` TYPE int PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "'DEFINE FIELD b ON `tb` TYPE string PERMISSIONS FULL'"

[[test.results]]
value = "[{ a: 5, b: 'x', id: tb:1 }, { a: -1, b: 'y', id: tb:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, b: 'x', c: 7, id: tb:1 }]"

[[test.results]]
value = "[tb:1]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_c'") && !string::contains(<string>$result, 'TableScan')'''
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD b ON tb TYPE string;
CREATE tb:1 SET a = '5', b = 'x' RETURN NONE;
CREATE tb:2 SET a = 'junk', b = 'y' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE string;
CREATE loose:1 SET a = '5' RETURN NONE;
DEFINE TABLE empty SCHEMAFULL;
DEFINE FIELD a ON empty TYPE string;
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
CREATE src:1 SET a = '5' RETURN NONE;
DEFINE TABLE v AS SELECT a FROM src;
(INFO FOR DB).compatibility;
SELECT * FROM tb ORDER BY id;
-- Level 4 to 3 with a row behind a step.
ALTER DATABASE test COMPATIBILITY 3;
(INFO FOR DB).compatibility;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.generation;
(INFO FOR TABLE tb).migrations.migrations;
(INFO FOR TABLE tb).migrations.guaranteed;
-- Level 3 accepts what level 4 refuses, and a migration still mints its generation.
ALTER FIELD b ON tb TYPE int;
DEFINE FIELD c ON tb TYPE int;
DEFINE INDEX by_c ON tb FIELDS c;
ALTER FIELD a ON tb TYPE string MIGRATE EXISTING ROWS AS a_str USING <string>$value FALLBACK '';
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
(INFO FOR TABLE tb).migrations.verification;
-- A cancelled raise leaves the level and the queue alone.
BEGIN;
ALTER DATABASE test COMPATIBILITY 4;
CANCEL;
(INFO FOR DB).compatibility;
(INFO FOR TABLE tb).migrations.verification;
-- Level 3 to 4: accepted at once, verified, and nothing is rewritten.
ALTER DATABASE test COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.generation;
(INFO FOR TABLE tb).migrations.migrations;
(INFO FOR TABLE tb).migrations.guaranteed;
(INFO FOR TABLE tb).migrations.verification;
(INFO FOR TABLE tb).migrations.progress.rewritten;
-- The tables that are not enforced, and the empty one, queue nothing.
(INFO FOR TABLE loose).migrations;
(INFO FOR TABLE empty).migrations;
(INFO FOR TABLE v).migrations;
(INFO FOR TABLE src).migrations.verification;
-- Enforced from then on.
ALTER FIELD b ON tb TYPE string;
ALTER FIELD a ON tb TYPE int;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int2 USING <int>$value FALLBACK -2;
SELECT VALUE a FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.generation;
-- Raising a level that is already 4 changes nothing.
ALTER DATABASE test COMPATIBILITY 4;
(INFO FOR TABLE tb).migrations.generation;
SELECT * FROM tb ORDER BY id;
-- A statement of a transaction is judged at the level the transaction has set.
BEGIN;
ALTER DATABASE test COMPATIBILITY 3;
ALTER FIELD b ON tb TYPE string;
ALTER DATABASE test COMPATIBILITY 4;
ALTER FIELD b ON tb TYPE int;
COMMIT;
(INFO FOR DB).compatibility;
(INFO FOR TABLE tb).fields.b;
BEGIN;
ALTER DATABASE test COMPATIBILITY 3;
ALTER FIELD b ON tb TYPE string;
COMMIT;
(INFO FOR DB).compatibility;
(INFO FOR TABLE tb).fields.b;
SELECT * FROM tb ORDER BY id;
-- An index defined at level 3 over a field is still the access path at level 4.
ALTER DATABASE test COMPATIBILITY 4;
UPDATE tb:1 SET c = 7 RETURN AFTER;
SELECT VALUE id FROM tb WHERE c = 7;
SELECT VALUE id FROM tb WHERE c = 7 EXPLAIN;
