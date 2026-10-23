/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"

[test]
reason = "While a row of an enforced table is behind a migration step, the table's TYPE cannot change and it cannot be redefined as a view, whatever the statement that asks: the table, its rows and its steps are left as they were, and every other change of the table is accepted. A TYPE that a record of the table does not admit is refused for that record before the step is judged, since waiting for the rewrite job would not let it through. A table that is not enforced accepts both, and what its reads return afterwards is the new table's: a relation's edges are read as they were, a view holds only the rows its source gives it, with nothing of the old rows or steps."

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
error = "Cannot make `tb` TYPE RELATION: it holds `tb`:1, which is not an edge. Delete the record first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot make `tb` TYPE RELATION: it holds `tb`:1, which is not an edge. Delete the record first, or define the table TYPE ANY"

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate table `ed`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot migrate table `ed`: rows are behind a migration step: the table's TYPE cannot change, nor can it become a view, until the rewrite job has finished"

[[test.results]]
error = "Cannot make `ed` TYPE NORMAL: it holds the edge ed:1. Delete the edge first, or define the table TYPE ANY"

[[test.results]]
error = "Cannot make `ed` TYPE NORMAL: it holds the edge ed:1. Delete the edge first, or define the table TYPE ANY"

[[test.results]]
value = "[{ a: 5, id: ed:1, in: pt:1, out: pt:2 }, { a: -1, id: ed:2, in: pt:1, out: pt:2 }]"

[[test.results]]
value = "'DEFINE TABLE ed TYPE ANY SCHEMAFULL PERMISSIONS NONE'"

[[test.results]]
value = "[{ field: 'a', generation: 3, name: 'a_int' }]"

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
value = "[{ a: 5, id: lo:1, in: pt:1, out: pt:2 }, { a: -1, id: lo:2, in: pt:1, out: pt:2 }]"

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
-- Enforced, rows behind a step: a TYPE the rows admit, or a view, is refused, however it is asked.
ALTER TABLE tb TYPE ANY;
DEFINE TABLE OVERWRITE tb TYPE ANY SCHEMAFULL;
DEFINE TABLE OVERWRITE tb AS SELECT a FROM src;
-- A TYPE a row does not admit is refused for that row before the step is judged: waiting for the
-- job would not let it through.
ALTER TABLE tb TYPE RELATION;
DEFINE TABLE OVERWRITE tb TYPE RELATION SCHEMAFULL;
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
-- Enforced, edges behind a step: the edges admit TYPE RELATION, so the step refuses it, however it
-- is asked; they do not admit TYPE NORMAL, which is refused for the edge.
DEFINE TABLE ed TYPE ANY SCHEMAFULL;
DEFINE FIELD a ON ed TYPE string;
RELATE pt:1->ed:1->pt:2 SET a = '5' RETURN NONE;
RELATE pt:1->ed:2->pt:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON ed TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER TABLE ed TYPE RELATION;
DEFINE TABLE OVERWRITE ed TYPE RELATION SCHEMAFULL;
ALTER TABLE ed TYPE NORMAL;
DEFINE TABLE OVERWRITE ed TYPE NORMAL SCHEMAFULL;
SELECT * FROM ed ORDER BY id;
(INFO FOR DB).tables.ed;
(INFO FOR TABLE ed).migrations.migrations;
-- With no step behind it a table takes both.
DEFINE TABLE nostep SCHEMAFULL;
DEFINE FIELD a ON nostep TYPE string;
CREATE nostep:1 SET a = '1' RETURN NONE;
ALTER TABLE nostep TYPE ANY;
(INFO FOR DB).tables.nostep;
DEFINE TABLE OVERWRITE nostep AS SELECT a FROM src;
SELECT * FROM nostep;
-- Not enforced (SCHEMALESS): a relation whose edges keep reading as they did.
DEFINE TABLE lo TYPE ANY SCHEMALESS;
DEFINE FIELD a ON lo TYPE string;
RELATE pt:1->lo:1->pt:2 SET a = '5' RETURN NONE;
RELATE pt:1->lo:2->pt:2 SET a = 'junk' RETURN NONE;
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
