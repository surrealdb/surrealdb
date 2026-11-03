/**
[env]
namespace = true
database = true

[test]
reason = "An output field that shadows a union field with a value of another kind orders by that value"

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
value = "[{ mood: 'HAPPY', n: 1 }, { mood: 'OK', n: 1 }, { mood: 'SAD', n: 1 }]"

[[test.results]]
value = "[{ mood: 'sad', n: 1 }, { mood: 'ok', n: 1 }, { mood: 'happy', n: 1 }]"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD mood ON person TYPE mood;
CREATE person:1 SET mood = 'sad';
CREATE person:2 SET mood = 'ok';
CREATE person:3 SET mood = 'happy';
SELECT string::uppercase(mood) AS mood, count() AS n FROM person GROUP BY mood ORDER BY mood;
SELECT mood, count() AS n FROM person GROUP BY mood ORDER BY mood;
