/**
[test]
reason = "A table removed and defined again under the same name starts without rows, steps, a queued rewrite or a migration name of its own: nothing the removed table held is read through the new definition, a field of the same name may take any other TYPE, and a migration of the same name is a new one. The generation the new table mints its first step at is above every generation the removed table minted. A table redefined with OVERWRITE keeps its rows and its steps."

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
value = "[]"

[[test.results]]
value = "[1005, 1006, 9]"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'tb' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, migrations: { generation: 5, guaranteed: [], migrations: [] }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['7', 'nine']"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ generation: 6, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[7, -1]"

[[test.results]]
value = "[{ field: 'a', generation: 7, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: [1, 2], id: tb:1 }]"

[[test.results]]
value = "{ generation: 10, guaranteed: [], migrations: [] }"

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
value = "[{ a: 1.5f, id: tb:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1.5f, id: tb:1 }]"

[[test.results]]
value = "{ generation: 13, guaranteed: ['a'], migrations: [] }"

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
value = "[1005]"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1005]"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'other'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "['5']"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6]"

[[test.results]]
match = "$result.database == 'test'"

[[test.results]]
value = "[1005]"

[[test.results]]
match = "$result.database == 'other'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'other'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 'str', id: ow:1 }]"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = '6' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 1000 FALLBACK -1;
CREATE tb:3 SET a = 9 RETURN NONE;
SELECT VALUE a FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
REMOVE TABLE tb;
SELECT * FROM tb;
-- Defined again, the table holds nothing of the removed one.
DEFINE TABLE tb SCHEMAFULL;
SELECT * FROM tb;
tb:1.a;
INFO FOR TABLE tb;
DEFINE FIELD a ON tb TYPE string;
-- The keys of the removed rows are free, and the new rows are read as written.
CREATE tb:1 SET a = '7' RETURN NONE;
CREATE tb:3 SET a = 'nine' RETURN NONE;
SELECT VALUE a FROM tb ORDER BY id;
SELECT VALUE type::is_string(a) FROM tb ORDER BY id;
SELECT VALUE a FROM tb:2;
(INFO FOR TABLE tb).migrations;
-- The name of the removed table's migration is free, and its step is the new table's.
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
SELECT VALUE a FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
-- The same name again, the field now of another TYPE altogether.
REMOVE TABLE tb;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE array<int>;
CREATE tb:1 SET a = [1, 2] RETURN NONE;
SELECT * FROM tb;
(INFO FOR TABLE tb).migrations;
-- Removed and defined again in one transaction.
ALTER FIELD a ON tb TYPE array<string> MIGRATE EXISTING ROWS AS a_str USING array::map($value, |$x| <string>$x) FALLBACK [];
BEGIN;
REMOVE TABLE tb;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE float;
CREATE tb:1 SET a = 1.5;
COMMIT;
SELECT * FROM tb;
(INFO FOR TABLE tb).migrations;
-- REMOVE TABLE IF EXISTS on a table that is gone does nothing.
REMOVE TABLE IF EXISTS gone;
-- A table redefined with OVERWRITE keeps its rows and its steps.
DEFINE TABLE ow SCHEMAFULL;
DEFINE FIELD a ON ow TYPE string;
CREATE ow:1 SET a = '5' RETURN NONE;
ALTER FIELD a ON ow TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 1000 FALLBACK -1;
DEFINE TABLE OVERWRITE ow TYPE NORMAL SCHEMAFULL;
SELECT VALUE a FROM ow;
(INFO FOR TABLE ow).migrations.migrations;
DEFINE TABLE OVERWRITE ow TYPE NORMAL SCHEMALESS;
SELECT VALUE a FROM ow;
(INFO FOR TABLE ow).migrations.migrations;
-- A table of the same name in another database has its own steps, and a database removed and defined again starts clean.
DEFINE DATABASE other;
USE DB other;
DEFINE TABLE ow SCHEMAFULL;
DEFINE FIELD a ON ow TYPE string;
CREATE ow:1 SET a = '5' RETURN NONE;
SELECT VALUE a FROM ow;
(INFO FOR TABLE ow).migrations;
ALTER FIELD a ON ow TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 1 FALLBACK -1;
SELECT VALUE a FROM ow;
USE DB test;
SELECT VALUE a FROM ow;
USE DB other;
REMOVE DATABASE other;
DEFINE DATABASE other;
USE DB other;
DEFINE TABLE ow SCHEMAFULL;
DEFINE FIELD a ON ow TYPE string;
SELECT * FROM ow;
CREATE ow:1 SET a = 'str' RETURN NONE;
SELECT * FROM ow;
(INFO FOR TABLE ow).migrations;
ALTER FIELD a ON ow TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 1 FALLBACK -1;
(INFO FOR TABLE ow).migrations.migrations;
