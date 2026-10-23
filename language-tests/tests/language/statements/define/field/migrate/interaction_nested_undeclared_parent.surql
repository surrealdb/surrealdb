/**
[test]
wip = true
reason = "Waits for the nesting rule: MIGRATE on a field whose parent is not declared is accepted without it. MIGRATE on a field whose parents are not all declared is refused, at every compatibility level: nothing is minted and the rows keep what they hold."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
match = "string::contains($error, 'Cannot migrate field `o.n`')"
error = true

[[test.results]]
match = '''$result.generation = 0 && array::len($result.migrations) = 0'''

[[test.results]]
value = "[{ id: w:1, o: { n: 1 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
match = "string::contains($error, 'Cannot migrate field `a.b.c`')"
error = true

[[test.results]]
match = '''$result.generation = 0 && array::len($result.migrations) = 0'''

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
match = "string::contains($error, 'Cannot migrate field `o.n`')"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, o: { n: 1 } }]"
*/
DEFINE TABLE w SCHEMAFULL;
DEFINE FIELD o.n ON w TYPE string;
CREATE w:1 SET o = { n: '1' } RETURN NONE;
ALTER FIELD o.n ON w TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE w).migrations;
SELECT * FROM w;
DEFINE TABLE y SCHEMAFULL;
DEFINE FIELD a ON y TYPE object;
DEFINE FIELD a.b.c ON y TYPE string;
CREATE y:1 SET a = { b: { c: '1' } } RETURN NONE;
ALTER FIELD a.b.c ON y TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE y).migrations;
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD o.n ON u TYPE string;
CREATE u:1 SET o = { n: '1' } RETURN NONE;
ALTER FIELD o.n ON u TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE u).migrations;
SELECT * FROM u;
