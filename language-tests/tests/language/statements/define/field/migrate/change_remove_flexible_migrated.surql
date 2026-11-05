/**
[test]
reason = "Change 6 with MIGRATE: a step that removes FLEXIBLE from a field of a SCHEMAFULL table must leave a value that holds no key no field declares, so a stored value with an undeclared key takes the FALLBACK, whether or not the field has fields beneath it."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ h: { k: 0 }, id: p:1 }, { h: { k: 2 }, id: p:2 }]"

[[test.results]]
value = "[{ g: {  }, id: p:1 }, { g: {  }, id: p:2 }]"

[[test.results]]
error = "Found field 'g.q', but no such field exists for table 'p'"

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
value = "NONE"

[[test.results]]
value = "[{ g: {  }, id: p:1 }]"
*/
-- Enforced table with rows. `h` has a field beneath it and `g` has none.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD g ON p TYPE object FLEXIBLE;
DEFINE FIELD h ON p TYPE object FLEXIBLE;
DEFINE FIELD h.k ON p TYPE int;
CREATE p:1 SET g = { q: 1 }, h = { k: 1, extra: 2 } RETURN NONE;
CREATE p:2 SET g = {}, h = { k: 2 } RETURN NONE;
ALTER FIELD g ON p DROP FLEXIBLE MIGRATE EXISTING ROWS AS g_strict USING $value FALLBACK {};
ALTER FIELD h ON p DROP FLEXIBLE MIGRATE EXISTING ROWS AS h_strict USING $value FALLBACK { k: 0 };
SELECT id, h FROM p ORDER BY id;
SELECT id, g FROM p ORDER BY id;
CREATE p:3 SET g = { q: 1 }, h = { k: 3 };
-- The same at compatibility level 3.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD g ON p TYPE object FLEXIBLE;
CREATE p:1 SET g = { q: 1 } RETURN NONE;
ALTER FIELD g ON p DROP FLEXIBLE MIGRATE EXISTING ROWS AS g_strict USING $value FALLBACK {};
SELECT id, g FROM p;
