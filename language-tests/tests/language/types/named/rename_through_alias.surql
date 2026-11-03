/**
[env]
namespace = true
database = true

[test]
reason = "RENAME VALUE rewrites the stored labels of a field typed through an alias or a domain of the union, at any depth, and an aggregated view refuses MIN and MAX of such a field"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'blue', f: 'blue', id: t:1, l: ['blue', 'ok'], m: 'blue' }, { f: 'ok', id: t:2, l: [], m: NULL }]"

[[test.results]]
value = "['blue']"

[[test.results]]
error = "Invalid statement: The view 'lows' cannot keep math::min(f): the field 'f' on the table 't' holds the labels of a union type, which order by declaration and not by number"

[[test.results]]
error = "Invalid statement: The view 'highs' cannot keep math::max(m): the field 'm' on the table 't' holds the labels of a union type, which order by declaration and not by number"

*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TYPE feeling AS mood;
DEFINE TYPE maybe_mood AS none | null | mood;
DEFINE TYPE again AS feeling;
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t TYPE feeling;
DEFINE FIELD m ON t TYPE maybe_mood;
DEFINE FIELD a ON t TYPE option<again>;
DEFINE FIELD l ON t TYPE array<feeling>;
CREATE t:1 SET f = 'sad', m = 'sad', a = 'sad', l = ['sad', 'ok'] RETURN NONE;
CREATE t:2 SET f = 'ok', m = NULL, l = [] RETURN NONE;
ALTER TYPE mood RENAME VALUE 'sad' TO 'blue';
SELECT * FROM t ORDER BY id;
UPDATE t:1 SET f = f RETURN VALUE f;
DEFINE TABLE lows AS SELECT math::min(f) AS lo FROM t GROUP ALL;
DEFINE TABLE highs AS SELECT math::max(m) AS hi FROM t GROUP ALL;
