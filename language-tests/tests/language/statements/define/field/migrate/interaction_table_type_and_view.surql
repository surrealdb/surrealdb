/**
[test]
reason = "While a row of an enforced table is behind a migration step, the table's TYPE cannot change and it cannot be redefined as a view, whatever the statement that asks: the table, its rows and its steps are left as they were, and every other change of the table is accepted. A table that is not enforced accepts both, and what its reads return afterwards is the new table's: a relation's rows are read as they were, a view holds only the rows its source gives it, with nothing of the old rows or steps."

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate table `tb`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `tb`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `tb`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `tb`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `tb`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
value = "[{ a: 5, id: tb:1 }, { a: -1, id: tb:2 }]"

[[test.results]]
value = "'DEFINE TABLE `tb` TYPE NORMAL SCHEMAFULL PERMISSIONS NONE'"

[[test.results]]
value = "{ generation: 4, guaranteed: [], migrations: [{ field: 'a', generation: 3, name: 'a_int' }], progress: { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE TABLE `tb` TYPE NORMAL SCHEMAFULL PERMISSIONS FULL'"

[[test.results]]
value = "[{ a: 5, id: tb:1 }, { a: -1, id: tb:2 }]"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate table `em`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE TABLE nostep TYPE ANY SCHEMAFULL PERMISSIONS NONE'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '7', id: nostep:1 }]"

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
value = "[{ a: 5, id: lo:1 }, { a: -1, id: lo:2 }]"

[[test.results]]
value = "[{ field: 'a', generation: 2, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '7', id: lo:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: '7', id: lo:1 }, { a: '8', id: lo:2 }]"

[[test.results]]
error = "Cannot migrate field `x` of table `lo`: a view's rows skip their field definitions"

[[test.results]]
error = "Cannot migrate field `a` of table `src`: the view `lo` selects from the table"

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
value = "[{ a: 5, id: tb:1 }, { a: -1, id: tb:2 }]"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '7', id: tb:1 }]"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
CREATE src:1 SET a = '7' RETURN NONE;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
-- Enforced, rows behind a step: refused, however it is asked.
ALTER TABLE tb TYPE RELATION;
ALTER TABLE tb TYPE ANY;
DEFINE TABLE OVERWRITE tb TYPE RELATION SCHEMAFULL;
DEFINE TABLE OVERWRITE tb TYPE ANY SCHEMAFULL;
DEFINE TABLE OVERWRITE tb AS SELECT a FROM src;
SELECT * FROM tb ORDER BY id;
(INFO FOR DB).tables.tb;
(INFO FOR TABLE tb).migrations;
-- Every other change of the table is accepted.
ALTER TABLE tb COMMENT 'kept';
ALTER TABLE tb TYPE NORMAL;
DEFINE TABLE OVERWRITE tb TYPE NORMAL SCHEMAFULL PERMISSIONS FULL;
(INFO FOR DB).tables.tb;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
-- A table whose steps await the job but that holds no row is refused too.
DEFINE TABLE em SCHEMAFULL;
DEFINE FIELD a ON em TYPE string;
ALTER FIELD a ON em TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER TABLE em TYPE ANY;
-- With no step behind it a table takes both.
DEFINE TABLE nostep SCHEMAFULL;
DEFINE FIELD a ON nostep TYPE string;
CREATE nostep:1 SET a = '1' RETURN NONE;
ALTER TABLE nostep TYPE ANY;
(INFO FOR DB).tables.nostep;
DEFINE TABLE OVERWRITE nostep AS SELECT a FROM src;
SELECT * FROM nostep;
-- Not enforced (SCHEMALESS): a relation whose rows keep reading as they did.
DEFINE TABLE lo SCHEMALESS;
DEFINE FIELD a ON lo TYPE string;
CREATE lo:1 SET a = '5' RETURN NONE;
CREATE lo:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON lo TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER TABLE lo TYPE RELATION;
SELECT * FROM lo ORDER BY id;
(INFO FOR TABLE lo).migrations.migrations;
-- Not enforced: a view holds the rows of its source, and nothing of the old rows or of their steps.
DEFINE TABLE OVERWRITE lo AS SELECT a FROM src;
SELECT * FROM lo ORDER BY id;
SELECT * FROM lo:2;
INFO FOR TABLE lo;
CREATE src:2 SET a = '8' RETURN NONE;
SELECT * FROM lo ORDER BY id;
-- A view's rows take no migration, and a source that has a view takes none either.
DEFINE FIELD x ON lo TYPE int MIGRATE EXISTING ROWS AS x_m USING 1 FALLBACK 1;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
CREATE src:1 SET a = '7' RETURN NONE;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER TABLE tb TYPE ANY;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations.migrations;
DEFINE TABLE OVERWRITE tb AS SELECT a FROM src;
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
