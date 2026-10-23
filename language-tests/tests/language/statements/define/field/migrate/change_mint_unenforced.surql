/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"

[test]
wip = true
reason = "Not built: a TYPE change without MIGRATE on an unenforced table mints no generation (the field is demoted to Unchecked and verified instead). Changes 5-7, 8-9 and 11 on a table that is not enforced: a TYPE change that is not a widening, a new field, and a COMPUTED field made stored are accepted and each mints the generation the field conforms from, while a widening mints nothing. Each table's next minted generation is observed through a migration that follows the change, since INFO FOR TABLE shows the migrations section only where a step exists or the table is enforced."

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'd3'"

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
value = "1"

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
value = "2"

[[test.results]]
value = "[{ failures: 0, field: 'a', finished: false }]"

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
value = "2"

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
value = "2"

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
value = "2"

[[test.results]]
value = "[{ failures: 0, field: 'n', finished: false }]"

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
value = "2"

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
value = "2"

[[test.results]]
match = "$result.database == 'test'"

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
value = "2"

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
value = "2"
*/
-- Compatibility level 3: no table is enforced.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
-- Control: a widening mints nothing, so the probe that follows mints generation 1.
DEFINE TABLE t0 SCHEMAFULL;
DEFINE FIELD a ON t0 TYPE int;
DEFINE FIELD q ON t0 TYPE int;
CREATE t0:1 SET a = 1, q = 1 RETURN NONE;
ALTER FIELD a ON t0 TYPE number;
ALTER FIELD q ON t0 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t0).migrations.generation;
-- 5 int to float.
DEFINE TABLE t5 SCHEMAFULL;
DEFINE FIELD a ON t5 TYPE int;
DEFINE FIELD q ON t5 TYPE int;
CREATE t5:1 SET a = 1, q = 1 RETURN NONE;
ALTER FIELD a ON t5 TYPE float;
ALTER FIELD q ON t5 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t5).migrations.generation;
(INFO FOR TABLE t5).migrations.verification;
-- 6 removing FLEXIBLE.
DEFINE TABLE t6 SCHEMAFULL;
DEFINE FIELD o ON t6 TYPE object FLEXIBLE;
DEFINE FIELD q ON t6 TYPE int;
CREATE t6:1 SET o = {}, q = 1 RETURN NONE;
ALTER FIELD o ON t6 DROP FLEXIBLE;
ALTER FIELD q ON t6 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t6).migrations.generation;
-- 7 narrowing a literal shape.
DEFINE TABLE t7 SCHEMAFULL;
DEFINE FIELD s ON t7 TYPE 'x' | 'y' | 'z';
DEFINE FIELD q ON t7 TYPE int;
CREATE t7:1 SET s = 'x', q = 1 RETURN NONE;
ALTER FIELD s ON t7 TYPE 'x' | 'y';
ALTER FIELD q ON t7 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t7).migrations.generation;
-- 8 a new field whose TYPE accepts NONE.
DEFINE TABLE t8 SCHEMAFULL;
DEFINE FIELD q ON t8 TYPE int;
CREATE t8:1 SET q = 1 RETURN NONE;
DEFINE FIELD n ON t8 TYPE option<int>;
ALTER FIELD q ON t8 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t8).migrations.generation;
(INFO FOR TABLE t8).migrations.verification;
-- 9 a new field whose TYPE does not.
DEFINE TABLE t9 SCHEMAFULL;
DEFINE FIELD q ON t9 TYPE int;
CREATE t9:1 SET q = 1 RETURN NONE;
DEFINE FIELD m ON t9 TYPE int;
ALTER FIELD q ON t9 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t9).migrations.generation;
-- 11 a COMPUTED field made stored.
DEFINE TABLE t11 SCHEMAFULL;
DEFINE FIELD q ON t11 TYPE int;
DEFINE FIELD c ON t11 COMPUTED q * 2;
CREATE t11:1 SET q = 1 RETURN NONE;
DEFINE FIELD OVERWRITE c ON t11 TYPE int;
ALTER FIELD q ON t11 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE t11).migrations.generation;
-- A SCHEMALESS table at the default level is not enforced either.
USE DB test;
DEFINE TABLE loose5 SCHEMALESS;
DEFINE FIELD a ON loose5 TYPE int;
DEFINE FIELD q ON loose5 TYPE int;
CREATE loose5:1 SET a = 1, q = 1 RETURN NONE;
ALTER FIELD a ON loose5 TYPE float;
ALTER FIELD q ON loose5 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE loose5).migrations.generation;
DEFINE TABLE loose8 SCHEMALESS;
DEFINE FIELD q ON loose8 TYPE int;
CREATE loose8:1 SET q = 1 RETURN NONE;
DEFINE FIELD n ON loose8 TYPE option<int>;
ALTER FIELD q ON loose8 MIGRATE EXISTING ROWS AS probe USING $value FALLBACK 0;
(INFO FOR TABLE loose8).migrations.generation;
