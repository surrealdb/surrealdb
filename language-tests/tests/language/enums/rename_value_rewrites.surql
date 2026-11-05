/**
[env]
namespace = true
database = true

[test]
reason = "RENAME VALUE rewrites the stored labels of every field that names the enum, in rows and in index entries, so records keep passing membership on their next write, and the rewrite fires no event"

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
value = "[{ mood: 'fine', name: 'a', past: ['fine', 'sad', 'fine'] }, { mood: 'happy', name: 'b', past: NONE }, { mood: NONE, name: 'c', past: NONE }]"

[[test.results]]
value = "[{ name: 'a' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = """{ attributes: { projections: 'name' }, children: [{ attributes: { access: "= 'fine'", direction: 'Forward', index: 'person_mood', table: 'person' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[{ feels: {'fine', 'happy'} }]"

[[test.results]]
value = "[{ mood: 'fine', name: 'a2' }]"

[[test.results]]
error = "Couldn't coerce value for field `mood` of `person:a`: Expected `mood ('sad' | 'fine' | 'happy')` but found `'ok'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ mood: 'okay', name: 'a2' }]"

[[test.results]]
error = "The type 'mood' already has the value 'happy'"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD mood ON person TYPE none | mood;
DEFINE FIELD past ON person TYPE none | array<mood>;
DEFINE INDEX person_mood ON person FIELDS mood;
DEFINE TABLE pet SCHEMAFULL;
DEFINE FIELD feels ON pet TYPE option<set<mood>>;
CREATE person:a SET name = 'a', mood = 'ok', past = ['ok', 'sad', 'ok'] RETURN NONE;
CREATE person:b SET name = 'b', mood = 'happy' RETURN NONE;
CREATE person:c SET name = 'c' RETURN NONE;
CREATE pet:p SET feels = ['ok', 'happy'] RETURN NONE;
DEFINE TABLE touched SCHEMALESS;
DEFINE EVENT audit ON person WHEN $event = 'UPDATE' THEN { CREATE touched SET who = $after.id };
DEFINE TABLE person_moods AS SELECT mood FROM person;
ALTER TYPE mood RENAME VALUE 'ok' TO 'fine';
SELECT VALUE who FROM touched ORDER BY who;
SELECT name, mood, past FROM person ORDER BY name;
SELECT name FROM person WHERE mood = 'fine';
SELECT name FROM person WHERE mood = 'ok';
SELECT name FROM person WHERE mood = 'fine' EXPLAIN;
SELECT feels FROM pet;
UPDATE person:a SET name = 'a2' RETURN name, mood;
UPDATE person:a SET mood = 'ok';
ALTER TYPE mood RENAME VALUE 'fine' TO 'x' RENAME VALUE 'x' TO 'okay';
SELECT name, mood FROM person WHERE mood = 'okay';
ALTER TYPE mood RENAME VALUE 'sad' TO 'happy';
