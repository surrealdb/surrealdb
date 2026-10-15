/**
[test]
reason = "A migration inside a transaction that is cancelled, or that fails, leaves nothing behind: no generation is minted, no step is stored, no rewrite is queued, every read is unchanged, and a later attempt, under the same name and with another transform, mints the first generation."

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
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '5', b: '6', id: tb:1 }]"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON `tb` TYPE string PERMISSIONS FULL', b: 'DEFINE FIELD b ON `tb` TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 3, guaranteed: [], migrations: [] }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ generation: 3, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "['5']"

[[test.results]]
value = "{ generation: 3, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Cannot migrate field `b` of table `tb`: the transform must have no effects: it may not read data, the catalog, the session, the clock or randomness"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ a: '5', b: '6', id: tb:1 }]"

[[test.results]]
value = "{ generation: 3, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "Couldn't coerce value for field `a` of ``tb`:3`: Expected `int` but found `'not an int'`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ a: '5', b: '6', id: tb:1 }]"

[[test.results]]
value = "{ generation: 3, guaranteed: [], migrations: [] }"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "['5']"

[[test.results]]
value = "{ generation: 3, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 105, b: '6', id: tb:1 }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "{ generation: 5, guaranteed: [], migrations: [{ field: 'a', generation: 4, name: 'a_int' }], progress: { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, id: tb:4 }]"

[[test.results]]
value = "[{ a: 105, b: 6, id: tb:1 }, { a: 1, b: 2, id: tb:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 105, b: 6, id: tb:1 }, { a: 1, b: 2, id: tb:4 }]"

[[test.results]]
value = "{ generation: 7, guaranteed: [], migrations: [{ field: 'a', generation: 4, name: 'a_int' }, { field: 'b', generation: 6, name: 'b_int' }], progress: { fallbacks: 0, lap: 3, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }, { failures: 0, field: 'b', finished: false }] }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
error = "The table 'nu' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['5']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "['5']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5]"

[[test.results]]
value = "4"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD b ON tb TYPE string;
CREATE tb:1 SET a = '5', b = '6' RETURN NONE;
-- BEGIN ... CANCEL.
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
CREATE tb:2 SET a = 7, b = '8';
CANCEL;
SELECT * FROM tb ORDER BY id;
INFO FOR TABLE tb;
(INFO FOR TABLE tb).migrations.progress;
-- Two migrations of one transaction, cancelled together.
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER FIELD b ON tb TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK -1;
CANCEL;
(INFO FOR TABLE tb).migrations;
-- A statement of the transaction that throws.
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
THROW 'boom';
COMMIT;
SELECT VALUE a FROM tb;
(INFO FOR TABLE tb).migrations;
-- A migration of the transaction that is refused fails the migration before it too.
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER FIELD b ON tb TYPE int MIGRATE EXISTING ROWS AS b_int USING time::now() FALLBACK -1;
COMMIT;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- A write of the transaction that is refused by the table fails the migration before it.
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
CREATE tb:3 SET a = 'not an int', b = 'x';
COMMIT;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- A statement that is not a transaction, and fails after its migration.
{ ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1; THROW 'boom'; };
SELECT VALUE a FROM tb;
(INFO FOR TABLE tb).migrations;
-- Nothing was reserved: the name is free and the first migration is generation 1, with another transform.
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 100 FALLBACK -1;
SELECT * FROM tb ORDER BY id;
SELECT VALUE type::is_int(a) FROM tb;
(INFO FOR TABLE tb).migrations;
-- A transaction that commits keeps the step, the writes and the queued rewrite.
BEGIN;
ALTER FIELD b ON tb TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK -1;
CREATE tb:4 SET a = 1, b = 2;
SELECT * FROM tb ORDER BY id;
COMMIT;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- A table, its field, its rows and its migration, all of a cancelled transaction.
BEGIN;
DEFINE TABLE nu SCHEMAFULL;
DEFINE FIELD z ON nu TYPE string;
CREATE nu:1 SET z = '1';
ALTER FIELD z ON nu TYPE int MIGRATE EXISTING ROWS AS z_int USING <int>$value FALLBACK 0;
CANCEL;
INFO FOR TABLE nu;
SELECT * FROM nu;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
CANCEL;
SELECT VALUE a FROM tb;
(INFO FOR TABLE tb).migrations;
BEGIN;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
THROW 'boom';
COMMIT;
SELECT VALUE a FROM tb;
(INFO FOR TABLE tb).migrations;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
SELECT VALUE a FROM tb;
(INFO FOR TABLE tb).migrations.generation;
