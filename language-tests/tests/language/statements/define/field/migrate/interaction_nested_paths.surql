/**
[test]
reason = "Nested definitions on an enforced table: a child behind a step of its own (an object member, array elements) reads, writes, indexes and feeds views with the migrated value; the nesting rule refuses a parent whose new kind strands a child, a literal shape that does not accept the child's type, and a child that does not fit its parent's shape; a FLEXIBLE parent is refused a step while a typed field is defined beneath it; no field beneath a step is guaranteed until the migration job has verified it; a level 3 database migrates the same paths and keeps REMOVE FIELD as it was."

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot give field `o` the type `none | int`: field `o.n` is defined in it, and only an object holds a field"

[[test.results]]
error = "Cannot set field `o.n` with type `none | string` as it mismatched with field `o` with type `none | { n: int }`"

[[test.results]]
value = "{ generation: 5, guaranteed: [], migrations: [] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''$result.guaranteed = [] && $result.migrations.map(|$m| $m.name) = ['on_int', 't_int'] && $result.migrations.map(|$m| $m.field) = ['o.n', 't']'''

[[test.results]]
value = "[{ id: doc:1, k: 'a', o: { n: 1 }, t: [1, 2] }, { id: doc:2, k: 'b', o: { n: 1 }, t: [3] }, { id: doc:3, k: 'c' }, { id: doc:4, k: 'd', o: { n: -1 }, t: [] }]"

[[test.results]]
value = "[[true, true], [true, true], [false, true], [true, true]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot set field `o.n` with type `none | int` as it mismatched with field `o` with type `none | { n: none | string }`"

[[test.results]]
value = "{ generation: 2, guaranteed: [], migrations: [] }"

[[test.results]]
value = "[{ id: doc:1, k: 'z', o: { n: 1 }, t: [1, 2] }]"

[[test.results]]
value = "[{ id: doc:5, k: 'e', o: { n: 9 }, t: [4, 5] }]"

[[test.results]]
error = "Couldn't coerce value for field `o.n` of `doc:2`: Expected `none | int` but found `'x'`"

[[test.results]]
error = "Couldn't coerce value for field `t` of `doc:2`: Expected `none | array<int>` but found `['x']`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_on'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[doc:1]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_t'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:1, doc:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[doc:2]"

[[test.results]]
value = "[doc:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: v:1, k: 'z', n: 8 }, { id: v:2, k: 'b', n: 1 }, { id: v:3, k: 'c', n: NONE }, { id: v:4, k: 'd', n: -1 }, { id: v:5, k: 'e', n: 9 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: v:1, k: 'z', n: 8 }, { id: v:2, k: 'b', n: 11 }, { id: v:3, k: 'c', n: NONE }, { id: v:4, k: 'd', n: -1 }, { id: v:5, k: 'e', n: 9 }]"

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
match = '''$result.guaranteed = [] && $result.migrations.map(|$m| $m.name) = ['on_int'] && $result.migrations.map(|$m| $m.field) = ['o.n']'''

[[test.results]]
error = "Cannot migrate field `o` of table `fx`: the type of the field `o.n` beneath it is not one a migrated value can be checked against: migrate that field itself"

[[test.results]]
match = '''$result.guaranteed = [] && $result.migrations.map(|$m| $m.name) = ['on_int'] && $result.migrations.map(|$m| $m.field) = ['o.n']'''

[[test.results]]
value = "[{ id: fx:1, o: { n: 1, x: 1 } }]"

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
match = '''$result.guaranteed = [] && $result.migrations.map(|$m| $m.name) = ['on_int', 'o_again'] && $result.migrations.map(|$m| $m.field) = ['o.n', 'o']'''

[[test.results]]
value = "[{ id: nf:1, o: { n: 1 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'lvl3'"

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
match = '''$result.guaranteed = [] && $result.migrations.map(|$m| $m.name) = ['on_int', 't_int'] && $result.migrations.map(|$m| $m.field) = ['o.n', 't']'''

[[test.results]]
value = "[{ id: doc:1, o: { n: 1 }, t: [1, 2] }]"

[[test.results]]
error = "Couldn't coerce value for field `o.n` of `doc:1`: Expected `none | int` but found `'x'`"

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
value = "[{ id: rm:1, o: { n: '1' } }]"
*/
-- (34, 28) Nested definitions on an enforced table. `o.n` sits under an object, `t.*` under an array.
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD k ON doc TYPE string;
DEFINE FIELD o ON doc TYPE option<object>;
DEFINE FIELD o.n ON doc TYPE option<string>;
DEFINE FIELD t ON doc TYPE option<array<string>>;
CREATE doc:1 SET k = 'a', o = { n: '1' }, t = ['1', '2'] RETURN NONE;
CREATE doc:2 SET k = 'b', o = { n: '1' }, t = ['3'] RETURN NONE;
CREATE doc:3 SET k = 'c' RETURN NONE;
CREATE doc:4 SET k = 'd', o = {}, t = [] RETURN NONE;
-- The nesting rule: a parent whose new kind has nowhere to hold the child is refused, and so is a literal shape that does not accept the child's type.
ALTER FIELD o ON doc TYPE option<int> MIGRATE EXISTING ROWS AS o_int USING <int>$value FALLBACK 0;
DEFINE FIELD OVERWRITE o ON doc TYPE option<{ n: int }> MIGRATE EXISTING ROWS AS o_shape USING $value FALLBACK NONE;
(INFO FOR TABLE doc).migrations;
-- The children migrate on their own paths.
ALTER FIELD o.n ON doc TYPE option<int> MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
DEFINE FIELD OVERWRITE t ON doc TYPE option<array<int>> MIGRATE EXISTING ROWS AS t_int USING array::map($value, |$x| <int>$x) FALLBACK NONE;
(INFO FOR TABLE doc).migrations;
SELECT * FROM doc ORDER BY id;
SELECT VALUE [type::is_int(o.n), array::all(t ?? [], |$x| type::is_int($x))] FROM doc ORDER BY id;
-- A child's own definition is checked against the shape its parent declares.
DEFINE TABLE sh SCHEMAFULL;
DEFINE FIELD o ON sh TYPE option<{ n: option<string> }>;
DEFINE FIELD o.n ON sh TYPE option<string>;
CREATE sh:1 SET o = { n: '1' } RETURN NONE;
ALTER FIELD o.n ON sh TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE sh).migrations;
-- Writes.
UPDATE doc:1 SET k = 'z' RETURN AFTER;
CREATE doc:5 SET k = 'e', o = { n: 9 }, t = [4, 5] RETURN AFTER;
UPDATE doc:2 SET o.n = 'x' RETURN AFTER;
UPDATE doc:2 SET t = ['x'] RETURN AFTER;
-- Indexes defined after the steps hold the migrated values. The element index is read through the flatten, the spelling that seeks it.
DEFINE INDEX by_on ON doc FIELDS o.n;
DEFINE INDEX by_t ON doc FIELDS t.*;
SELECT VALUE id FROM doc WHERE o.n = 1 ORDER BY id;
SELECT VALUE id FROM doc WHERE o.n = 1 EXPLAIN;
SELECT VALUE id FROM doc WHERE t.* CONTAINS 2 ORDER BY id;
SELECT VALUE id FROM doc WHERE t.* CONTAINS 2 EXPLAIN;
UPDATE doc:2 SET t = [2, 7] RETURN NONE;
SELECT VALUE id FROM doc WHERE t.* CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM doc WHERE t.* CONTAINS 2 ORDER BY id;
UPDATE doc:1 SET o.n = 8 RETURN NONE;
SELECT VALUE id FROM doc WHERE o.n = 1 ORDER BY id;
SELECT VALUE id FROM doc WHERE o.n = 8 ORDER BY id;
-- A view over a nested path sees the migrated value.
DEFINE TABLE v AS SELECT o.n AS n, k FROM doc;
SELECT * FROM v ORDER BY id;
UPDATE doc:2 SET o.n = 11 RETURN NONE;
SELECT * FROM v ORDER BY id;
-- A FLEXIBLE parent holds keys no definition declares, so no type a migrated value is tested against can carry a typed field beneath it: its step is refused, and the field migrates on its own path.
DEFINE TABLE fx SCHEMAFULL;
DEFINE FIELD o ON fx TYPE option<object> FLEXIBLE;
DEFINE FIELD o.n ON fx TYPE option<string>;
CREATE fx:1 SET o = { n: '1', x: 1 } RETURN NONE;
ALTER FIELD o.n ON fx TYPE option<int> MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE fx).migrations;
DEFINE FIELD OVERWRITE o ON fx TYPE option<object> FLEXIBLE MIGRATE EXISTING ROWS AS o_again USING $value FALLBACK NONE;
(INFO FOR TABLE fx).migrations;
SELECT * FROM fx;
DEFINE TABLE nf SCHEMAFULL;
DEFINE FIELD o ON nf TYPE option<object>;
DEFINE FIELD o.n ON nf TYPE option<string>;
CREATE nf:1 SET o = { n: '1' } RETURN NONE;
ALTER FIELD o.n ON nf TYPE option<int> MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
DEFINE FIELD OVERWRITE o ON nf TYPE option<object> MIGRATE EXISTING ROWS AS o_again USING $value FALLBACK NONE;
(INFO FOR TABLE nf).migrations;
SELECT * FROM nf;
-- Level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE doc SCHEMAFULL;
DEFINE FIELD o ON doc TYPE option<object>;
DEFINE FIELD o.n ON doc TYPE option<string>;
DEFINE FIELD t ON doc TYPE option<array<string>>;
CREATE doc:1 SET o = { n: '1' }, t = ['1', '2'] RETURN NONE;
(INFO FOR TABLE doc).migrations;
ALTER FIELD o.n ON doc TYPE option<int> MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
DEFINE FIELD OVERWRITE t ON doc TYPE option<array<int>> MIGRATE EXISTING ROWS AS t_int USING array::map($value, |$x| <int>$x) FALLBACK NONE;
(INFO FOR TABLE doc).migrations;
SELECT * FROM doc;
UPDATE doc:1 SET o.n = 'x' RETURN AFTER;
DEFINE TABLE rm SCHEMAFULL;
DEFINE FIELD o ON rm TYPE option<object>;
DEFINE FIELD o.n ON rm TYPE option<string>;
CREATE rm:1 SET o = { n: '1' } RETURN NONE;
REMOVE FIELD o ON rm;
(INFO FOR TABLE rm).migrations;
SELECT * FROM rm;
