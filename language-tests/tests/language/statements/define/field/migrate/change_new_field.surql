/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"

[test]
reason = "Changes 8-10: a new field whose TYPE accepts NONE is accepted by every table (an enforced one queues its verification), a new field whose TYPE does not accept NONE is refused by an enforced table with rows unless it carries MIGRATE, and a new field with MIGRATE fills every old row; an enforced table with no rows, a SCHEMALESS table and a table at compatibility level 3 accept every new field and leave old rows without a value."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "2"

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
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ failures: 0, field: 'n1', finished: false }, { failures: 0, field: 'n2', finished: false }, { failures: 0, field: 'n3', finished: false }, { failures: 0, field: 'n4', finished: false }, { failures: 0, field: 'n5', finished: false }]"

[[test.results]]
value = "[{ a: 1, id: p:1, z: 2 }, { a: 2, id: p:2, z: 3 }]"

[[test.results]]
error = "Cannot change field `m1` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
error = "Cannot change field `m2` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
error = "Cannot change field `m3` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
error = "Cannot change field `m4` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
error = "Cannot change field `m5` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
error = "Cannot change field `m6` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
error = "Cannot change field `m7` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
value = """{ a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', n1: 'DEFINE FIELD n1 ON p TYPE none | int PERMISSIONS FULL', n2: "DEFINE FIELD n2 ON p TYPE none | string DEFAULT 'd' PERMISSIONS FULL", n3: 'DEFINE FIELD n3 ON p TYPE any PERMISSIONS FULL', n4: 'DEFINE FIELD n4 ON p TYPE none | int PERMISSIONS FULL', n5: 'DEFINE FIELD n5 ON p TYPE none | int READONLY PERMISSIONS FULL', z: 'DEFINE FIELD z ON p COMPUTED a + 1 PERMISSIONS FULL' }"""

[[test.results]]
value = "7"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "13"

[[test.results]]
value = "[{ field: 'k1', generation: 8, name: 'add_k1' }, { field: 'k2', generation: 10, name: 'add_k2' }, { field: 'k3', generation: 12, name: 'add_k3' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: p:1, k1: 5, k1i: true, k2: 'x', k3: 6, z: 2 }, { a: 2, id: p:2, k1: 5, k1i: true, k2: 'x', k3: 6, z: 3 }]"

[[test.results]]
error = "Couldn't coerce value for field `k1` of `p:3`: Expected `int` but found `NONE`"

[[test.results]]
value = "[{ a: 3, id: p:3, k1: 7, k2: 'd', n2: 'd', z: 4 }]"

[[test.results]]
value = "[{ a: 3, id: p:3, k1: 7, k2: 'd', n2: 'd', z: 4 }]"

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
value = """{ a: 'DEFINE FIELD a ON e TYPE int PERMISSIONS FULL', m1: 'DEFINE FIELD m1 ON e TYPE int PERMISSIONS FULL', m2: "DEFINE FIELD m2 ON e TYPE string DEFAULT 'd' PERMISSIONS FULL", n1: 'DEFINE FIELD n1 ON e TYPE none | int PERMISSIONS FULL' }"""

[[test.results]]
value = "5"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "[{ a: 1, id: e:1, k1: 1, m1: 1, m2: 'd' }]"

[[test.results]]
value = "[{ a: 1, id: e:1, k1: 1, m1: 1, m2: 'd' }]"

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
value = "[{ a: 1, id: loose:1, n: 'text', note: 'hi', ns: true }, { a: 2, id: loose:2, note: 'none', ns: false }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'd3'"

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
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: p:1, k1: 5 }, { a: 2, id: p:2, k1: 5 }]"

[[test.results]]
error = "Couldn't coerce value for field `k1` of `p:3`: Expected `int` but found `NONE`"

[[test.results]]
value = "[{ a: 3, id: p:3, k1: 1, m1: 1, m2: 'd' }]"

[[test.results]]
value = "[{ a: 3, id: p:3, k1: 1, m1: 1, m2: 'd' }]"
*/
-- Enforced table with rows.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
CREATE p:1 SET a = 1 RETURN NONE;
CREATE p:2 SET a = 2 RETURN NONE;
(INFO FOR TABLE p).migrations.generation;
-- 8 A new field whose TYPE accepts NONE.
DEFINE FIELD n1 ON p TYPE option<int>;
DEFINE FIELD n2 ON p TYPE option<string> DEFAULT 'd';
DEFINE FIELD n3 ON p TYPE any;
DEFINE FIELD IF NOT EXISTS n4 ON p TYPE option<int>;
DEFINE FIELD OVERWRITE n5 ON p TYPE option<int> READONLY;
DEFINE FIELD z ON p COMPUTED a + 1;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
(INFO FOR TABLE p).migrations.verification;
SELECT * FROM p ORDER BY id;
-- 9 A new field whose TYPE does not.
DEFINE FIELD m1 ON p TYPE int;
DEFINE FIELD m2 ON p TYPE string DEFAULT 'd';
DEFINE FIELD m3 ON p TYPE int | string;
DEFINE FIELD IF NOT EXISTS m4 ON p TYPE int;
DEFINE FIELD OVERWRITE m5 ON p TYPE int;
DEFINE FIELD m6 ON p TYPE int VALUE 5;
DEFINE FIELD m7 ON p TYPE int DEFAULT ALWAYS 5;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
-- 10 With MIGRATE every old row gets a value, and the field is Guaranteed.
DEFINE FIELD k1 ON p TYPE int MIGRATE EXISTING ROWS AS add_k1 USING 5;
DEFINE FIELD k2 ON p TYPE string DEFAULT 'd' MIGRATE EXISTING ROWS AS add_k2 USING 'x';
DEFINE FIELD k3 ON p TYPE option<int> MIGRATE EXISTING ROWS AS add_k3 USING 6;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.migrations;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT *, type::is_int(k1) AS k1i FROM p ORDER BY id;
-- A row written afterwards holds what its definitions say.
CREATE p:3 SET a = 3;
CREATE p:3 SET a = 3, k1 = 7;
SELECT * FROM p:3;
-- An enforced table with no rows needs no migration: every new field is
-- accepted, and only a migration mints.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD n1 ON e TYPE option<int>;
DEFINE FIELD m1 ON e TYPE int;
DEFINE FIELD m2 ON e TYPE string DEFAULT 'd';
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
(INFO FOR TABLE e).migrations.guaranteed;
(INFO FOR TABLE e).migrations.verification;
DEFINE FIELD k1 ON e TYPE int MIGRATE EXISTING ROWS AS add_k1 USING 5;
(INFO FOR TABLE e).migrations.generation;
CREATE e:1 SET a = 1, m1 = 1, k1 = 1;
SELECT * FROM e;
-- A SCHEMALESS table accepts every new field and may hold values at its path.
DEFINE TABLE loose SCHEMALESS;
CREATE loose:1 SET a = 1, note = 'hi', n = 'text' RETURN NONE;
CREATE loose:2 SET a = 2 RETURN NONE;
DEFINE FIELD n ON loose TYPE int;
DEFINE FIELD m1 ON loose TYPE int;
DEFINE FIELD note ON loose TYPE string MIGRATE EXISTING ROWS AS add_note USING $value ?? 'none' FALLBACK 'none';
SELECT *, type::is_string(n) AS ns FROM loose ORDER BY id;
(INFO FOR TABLE loose).migrations.generation;
(INFO FOR TABLE loose).migrations.guaranteed;
-- At compatibility level 3 no table is enforced.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
CREATE p:1 SET a = 1 RETURN NONE;
CREATE p:2 SET a = 2 RETURN NONE;
DEFINE FIELD n1 ON p TYPE option<int>;
DEFINE FIELD m1 ON p TYPE int;
DEFINE FIELD m2 ON p TYPE string DEFAULT 'd';
DEFINE FIELD k1 ON p TYPE int MIGRATE EXISTING ROWS AS add_k1 USING 5;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
CREATE p:3 SET a = 3, m1 = 1;
CREATE p:3 SET a = 3, m1 = 1, k1 = 1;
SELECT * FROM p:3;
