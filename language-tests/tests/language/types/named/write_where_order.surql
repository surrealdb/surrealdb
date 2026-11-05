/**
[env]
namespace = true
database = true

[test]
reason = "The WHERE of an UPDATE, DELETE or UPSERT compares a union field by the declaration order of its labels"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, mood: 'sad' }]"

[[test.results]]
value = "[{ id: person:2, mood: 'ok' }]"

[[test.results]]
value = "[{ id: person:3, mood: 'happy' }]"

[[test.results]]
value = "[{ id: person:4, mood: 'happy' }]"

[[test.results]]
value = "[{ id: person:1, mood: 'sad' }]"

[[test.results]]
value = "[{ id: person:3, mood: 'ok' }]"

[[test.results]]
value = "[{ id: person:4, mood: 'sad' }]"

[[test.results]]
value = "[[person:2, 'ok'], [person:3, 'ok'], [person:4, 'sad']]"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD mood ON person TYPE mood;
CREATE person:1 SET mood = 'sad';
CREATE person:2 SET mood = 'ok';
CREATE person:3 SET mood = 'happy';
CREATE person:4 SET mood = 'happy';
DELETE person WHERE mood < 'ok' RETURN BEFORE;
UPDATE person SET mood = 'ok' WHERE mood > 'ok' AND id = person:3 RETURN AFTER;
UPSERT person:4 SET mood = 'sad' WHERE mood >= 'happy' RETURN AFTER;
SELECT VALUE [id, mood] FROM person ORDER BY id;
