/**
[test]
reason = "Changes 5-7: a TYPE change that is not a widening (int to float, removing FLEXIBLE, narrowing a literal shape, T | U to T) is refused without MIGRATE by an enforced table with rows, accepted with MIGRATE (minting one generation) and accepted without it by an enforced table with no rows, a SCHEMALESS table and a table at compatibility level 3, which keep reading old values as stored."

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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "7"

[[test.results]]
error = "Cannot change field `a` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `g` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `s` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `t` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `u` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `w` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `a` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `a` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = """{ a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE object FLEXIBLE PERMISSIONS FULL', s: "DEFINE FIELD s ON p TYPE 'x' | 'y' | 'z' PERMISSIONS FULL", t: 'DEFINE FIELD t ON p TYPE { k: int, l: string } PERMISSIONS FULL', u: 'DEFINE FIELD u ON p TYPE none | int PERMISSIONS FULL', w: 'DEFINE FIELD w ON p TYPE number PERMISSIONS FULL' }"""

[[test.results]]
value = "7"

[[test.results]]
value = "[{ a: 1, ai: true, g: {  }, id: p:1, s: 'x', t: { k: 1, l: 'l' }, u: 1, w: 2, wi: true }, { a: 2, ai: true, g: {  }, id: p:2, s: 'z', t: { k: 2, l: 'm' }, u: NONE, w: 2.5f, wi: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ a: 'DEFINE FIELD a ON p TYPE float PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE object PERMISSIONS FULL', s: "DEFINE FIELD s ON p TYPE 'x' | 'y' PERMISSIONS FULL", t: 'DEFINE FIELD t ON p TYPE { k: int, l: string } PERMISSIONS FULL', u: 'DEFINE FIELD u ON p TYPE none | int PERMISSIONS FULL', w: 'DEFINE FIELD w ON p TYPE number PERMISSIONS FULL' }"""

[[test.results]]
value = "12"

[[test.results]]
value = "[{ field: 'a', generation: 8, name: 'a_float' }, { field: 'g', generation: 10, name: 'g_strict' }, { field: 's', generation: 11, name: 's_narrow' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1f, af: true, g: {  }, id: p:1, s: 'x' }, { a: 2f, af: true, g: {  }, id: p:2, s: 'x' }]"

[[test.results]]
error = "Found field 'g.z', but no such field exists for table 'p'"

[[test.results]]
error = "Couldn't coerce value for field `s` of `p:4`: Expected `'x' | 'y'` but found `'z'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 3f, af: true, s: 'y' }"

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
value = """{ a: 'DEFINE FIELD a ON e TYPE float PERMISSIONS FULL', g: 'DEFINE FIELD g ON e TYPE object PERMISSIONS FULL', s: "DEFINE FIELD s ON e TYPE 'x' | 'y' PERMISSIONS FULL", t: 'DEFINE FIELD t ON e TYPE { k: int } PERMISSIONS FULL', u: 'DEFINE FIELD u ON e TYPE int PERMISSIONS FULL' }"""

[[test.results]]
value = "9"

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
value = """{ a: 'DEFINE FIELD a ON loose TYPE float PERMISSIONS FULL', s: "DEFINE FIELD s ON loose TYPE 'x' | 'y' PERMISSIONS FULL" }"""

[[test.results]]
value = "[{ a: 1, ai: true, id: loose:1, s: 'z' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 2f, af: true, id: loose:2, s: 'y' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'd3', namespace: 'test' }"

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
value = """{ a: 'DEFINE FIELD a ON p TYPE float PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE object PERMISSIONS FULL', s: "DEFINE FIELD s ON p TYPE 'x' | 'y' PERMISSIONS FULL", t: 'DEFINE FIELD t ON p TYPE { k: int } PERMISSIONS FULL', u: 'DEFINE FIELD u ON p TYPE int PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ a: 1, ai: true, g: { q: 1 }, id: p:1, s: 'z', t: { k: 1, l: 'l' }, u: NONE }]"

[[test.results]]
error = "Found field 'g.q', but no such field exists for table 'p'"

[[test.results]]
error = "Couldn't coerce value for field `s` of `p:3`: Expected `'x' | 'y'` but found `'z'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 2f, af: true }"
*/
-- Enforced table with rows.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD g ON p TYPE object FLEXIBLE;
DEFINE FIELD s ON p TYPE 'x' | 'y' | 'z';
DEFINE FIELD t ON p TYPE { k: int, l: string };
DEFINE FIELD u ON p TYPE option<int>;
DEFINE FIELD w ON p TYPE number;
CREATE p:1 SET a = 1, g = {}, s = 'x', t = { k: 1, l: 'l' }, u = 1, w = 2 RETURN NONE;
CREATE p:2 SET a = 2, g = {}, s = 'z', t = { k: 2, l: 'm' }, u = NONE, w = 2.5 RETURN NONE;
(INFO FOR TABLE p).migrations.generation;
-- 5 int to float, 6 removing FLEXIBLE, 7 narrowing a literal shape, and the
-- other retypes that are no widening.
ALTER FIELD a ON p TYPE float;
ALTER FIELD g ON p DROP FLEXIBLE;
ALTER FIELD s ON p TYPE 'x' | 'y';
ALTER FIELD t ON p TYPE { k: int };
ALTER FIELD u ON p TYPE int;
ALTER FIELD w ON p TYPE int;
ALTER FIELD a ON p TYPE string;
DEFINE FIELD OVERWRITE a ON p TYPE float;
-- Nothing changed.
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
SELECT id, a, type::is_int(a) AS ai, g, s, t, u, w, type::is_int(w) AS wi FROM p ORDER BY id;
-- With MIGRATE each is accepted and mints one generation.
ALTER FIELD a ON p TYPE float MIGRATE EXISTING ROWS AS a_float USING <float>$value FALLBACK 0;
ALTER FIELD g ON p DROP FLEXIBLE MIGRATE EXISTING ROWS AS g_strict USING $value FALLBACK {};
ALTER FIELD s ON p TYPE 'x' | 'y' MIGRATE EXISTING ROWS AS s_narrow USING $value FALLBACK 'x';
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.migrations;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT id, a, type::is_float(a) AS af, g, s FROM p ORDER BY id;
-- Rows written afterwards are held to the new definitions.
CREATE p:3 SET a = 3, g = { z: 1 }, s = 'y', t = { k: 3, l: 'n' }, u = 3, w = 3;
CREATE p:4 SET a = 3, g = {}, s = 'z', t = { k: 3, l: 'n' }, u = 3, w = 3;
CREATE p:5 SET a = 3, g = {}, s = 'y', t = { k: 3, l: 'n' }, u = 3, w = 3 RETURN NONE;
SELECT a, type::is_float(a) AS af, s FROM ONLY p:5;
(INFO FOR TABLE p).migrations.generation;
-- An enforced table with no rows needs no migration, and mints nothing.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD g ON e TYPE object FLEXIBLE;
DEFINE FIELD s ON e TYPE 'x' | 'y' | 'z';
DEFINE FIELD t ON e TYPE { k: int, l: string };
DEFINE FIELD u ON e TYPE option<int>;
ALTER FIELD a ON e TYPE float;
ALTER FIELD g ON e DROP FLEXIBLE;
ALTER FIELD s ON e TYPE 'x' | 'y';
ALTER FIELD t ON e TYPE { k: int };
ALTER FIELD u ON e TYPE int;
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
-- A SCHEMALESS table accepts every retype and reads old rows as stored.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE int;
DEFINE FIELD s ON loose TYPE 'x' | 'y' | 'z';
CREATE loose:1 SET a = 1, s = 'z' RETURN NONE;
ALTER FIELD a ON loose TYPE float;
ALTER FIELD s ON loose TYPE 'x' | 'y';
(INFO FOR TABLE loose).fields;
SELECT id, a, type::is_int(a) AS ai, s FROM loose;
CREATE loose:2 SET a = 2, s = 'y' RETURN NONE;
SELECT id, a, type::is_float(a) AS af, s FROM loose:2;
-- At compatibility level 3 no table is enforced.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD g ON p TYPE object FLEXIBLE;
DEFINE FIELD s ON p TYPE 'x' | 'y' | 'z';
DEFINE FIELD t ON p TYPE { k: int, l: string };
DEFINE FIELD u ON p TYPE option<int>;
CREATE p:1 SET a = 1, g = { q: 1 }, s = 'z', t = { k: 1, l: 'l' }, u = NONE RETURN NONE;
ALTER FIELD a ON p TYPE float;
ALTER FIELD g ON p DROP FLEXIBLE;
ALTER FIELD s ON p TYPE 'x' | 'y';
ALTER FIELD t ON p TYPE { k: int };
ALTER FIELD u ON p TYPE int;
(INFO FOR TABLE p).fields;
SELECT id, a, type::is_int(a) AS ai, g, s, t, u FROM p;
CREATE p:2 SET a = 2, g = { q: 1 }, s = 'y', t = { k: 2 }, u = 2;
CREATE p:3 SET a = 2, g = {}, s = 'z', t = { k: 2 }, u = 2;
CREATE p:4 SET a = 2, g = {}, s = 'y', t = { k: 2 }, u = 2 RETURN NONE;
SELECT a, type::is_float(a) AS af FROM ONLY p:4;
