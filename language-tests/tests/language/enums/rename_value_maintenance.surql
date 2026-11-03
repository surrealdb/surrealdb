/**
[env]
namespace = true
database = true

[test]
reason = "RENAME VALUE rewrites stored labels as engine maintenance: a READONLY field is rewritten, no EVENT fires, no VALUE or DEFAULT ALWAYS is evaluated, and only the records holding a renamed label are written. A label added earlier in the same edit list holds no stored value, so renaming it rewrites nothing, and a function body that spells an enum's type in a string does not hold the enum"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ mood: 'fine', name: 'a', tag: NONE, writes: 1 }, { mood: 'happy', name: 'b', tag: NONE, writes: 1 }]"

# The rewrite is one more write of person:a, and none of person:b.
[[test.results]]
match = '''
array::len(array::filter(array::flatten($result.changes), |$c| $c.update.id == person:a)) == 2
&& array::len(array::filter(array::flatten($result.changes), |$c| $c.update.id == person:b)) == 1
&& array::any(array::flatten($result.changes), |$c| $c.update == { id: person:a, mood: 'fine', name: 'a', writes: 1 })
'''
error = false

[[test.results]]
error = "Found changed value for field `mood`, with record `person:a`, but field is readonly"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['sad', 'well', 'happy', 'great']"

[[test.results]]
value = "[{ mood: 'well', name: 'a' }, { mood: 'happy', name: 'b' }]"

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
error = "Invalid statement: Cannot remove the type 'tone': the function 'fn::cast' names it"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL CHANGEFEED 1h;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD mood ON person TYPE mood READONLY;
DEFINE FIELD writes ON person TYPE int VALUE ($before.writes ?? 0) + 1;
CREATE person:a SET name = 'a', mood = 'ok' RETURN NONE;
CREATE person:b SET name = 'b', mood = 'happy' RETURN NONE;
DEFINE FIELD tag ON person TYPE option<string> DEFAULT ALWAYS 'stamped';
DEFINE TABLE touched SCHEMALESS;
DEFINE EVENT audit ON person THEN { CREATE touched SET who = $after.id };
ALTER TYPE mood RENAME VALUE 'ok' TO 'fine';
SELECT VALUE who FROM touched;
SELECT name, mood, writes, tag FROM person ORDER BY name;
SHOW CHANGES FOR TABLE person SINCE 1;
UPDATE person:a SET mood = 'sad';
ALTER TYPE mood RENAME VALUE 'fine' TO 'well' ADD VALUE 'fine' RENAME VALUE 'fine' TO 'great';
enum::values('mood');
SELECT name, mood FROM person ORDER BY name;
DEFINE TYPE tone AS 'low' | 'high';
DEFINE FUNCTION fn::spell() { RETURN "tone" };
REMOVE TYPE tone;
DEFINE TYPE tone AS 'low' | 'high';
DEFINE FUNCTION fn::cast($v: any) { RETURN <option<tone>> $v };
REMOVE TYPE tone;
