/**
[test]
wip = true
reason = "Waits for the nesting rule: REMOVE FIELD does not cascade to the definitions beneath it without it. REMOVE FIELD cascades on an enforced table: the definitions beneath the removed field go with it, each field the cascade removes gets a removal step, and the table no longer admits the removed path."

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
value = "{}"

[[test.results]]
match = '''array::sort($result.migrations.map(|$m| $m.field)) = ['o', 'o.n'] && $result.generation = array::len($result.migrations)'''

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
match = "string::contains($error, \"Found field 'o'\")"
error = true
*/
DEFINE TABLE a SCHEMAFULL;
DEFINE FIELD o ON a TYPE option<object>;
DEFINE FIELD o.n ON a TYPE option<string>;
CREATE a:1 SET o = { n: '1' } RETURN NONE;
REMOVE FIELD o ON a;
(INFO FOR TABLE a).fields;
(INFO FOR TABLE a).migrations;
SELECT * FROM a;
CREATE a:2 SET o = { n: 'x' } RETURN AFTER;
