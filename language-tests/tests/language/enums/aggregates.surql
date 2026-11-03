/**
[env]
namespace = true
database = true

[test]
reason = "MIN and MAX aggregates over an enum field answer the label that ranks first or last, and the scalar forms keep working"

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
value = "[{ hi: 'happy', lo: 'sad' }]"

[[test.results]]
value = "[{ hi: 'happy', lo: 'ok', team: 'x' }, { hi: 'sad', lo: 'sad', team: 'y' }]"

[[test.results]]
value = "[{ hi: 'happy', team: 'x' }, { hi: 'sad', team: 'y' }]"

[[test.results]]
value = "'ok'"

[[test.results]]
value = "'ok'"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD team ON person TYPE string;
DEFINE FIELD mood ON person TYPE none | mood;
CREATE person:a SET name = 'a', team = 'x', mood = 'happy' RETURN NONE;
CREATE person:b SET name = 'b', team = 'x', mood = 'ok' RETURN NONE;
CREATE person:c SET name = 'c', team = 'y', mood = 'sad' RETURN NONE;
CREATE person:d SET name = 'd', team = 'y' RETURN NONE;
SELECT math::min(mood) AS lo, math::max(mood) AS hi FROM person GROUP ALL;
SELECT team, array::min(mood) AS lo, array::max(mood) AS hi FROM person GROUP BY team ORDER BY team;
SELECT team, math::max(mood) AS hi FROM person GROUP BY team ORDER BY hi DESC;
RETURN enum::min('mood', ['happy', 'ok']);
RETURN enum::max('mood', ['sad', 'ok']);
