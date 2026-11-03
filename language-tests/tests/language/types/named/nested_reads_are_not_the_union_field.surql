/**
[env]
namespace = true
database = true

[test]
reason = "A field read inside an element filter or a graph hop's condition is not the table's union field of the same name"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, mood: 'ok', notes: [{ mood: 'zebra' }, { mood: 'apple' }] }]"

[[test.results]]
value = "[{ id: person:2, mood: 'sad' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[{ mood: 'apple' }]]"

[[test.results]]
value = "[['aardvark']]"

[[test.results]]
value = "[]"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD mood ON person TYPE mood;
DEFINE FIELD notes ON person TYPE option<array<{ mood: string }>>;
CREATE person:1 SET mood = 'ok', notes = [{ mood: 'zebra' }, { mood: 'apple' }];
CREATE person:2 SET mood = 'sad';
RELATE person:1->likes->person:2 SET mood = 'aardvark' RETURN NONE;
RELATE person:1->likes->person:2 SET mood = 'yak' RETURN NONE;
SELECT VALUE notes[WHERE mood < 'm'] FROM person:1;
SELECT VALUE ->likes[WHERE mood < 'm'].mood FROM person:1;
SELECT VALUE id FROM person WHERE mood < 'ok' AND notes[WHERE mood < 'm'] = [];
