/**
[env]
namespace = true
database = true

[test]
reason = "An aggregated view does not keep the MIN or MAX of a union field, whether or not the table holds rows"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid statement: The view 'top' cannot keep math::max(mood): the field 'mood' on the table 'person' holds the labels of a union type, which order by declaration and not by number"

[[test.results]]
value = "[{ id: person:1, mood: 'ok' }]"

[[test.results]]
error = "Invalid statement: The view 'bottom' cannot keep math::min(mood): the field 'mood' on the table 'person' holds the labels of a union type, which order by declaration and not by number"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: counted:['ok'], mood: 'ok', n: 1 }]"

[[test.results]]
value = "[{ top: 'ok' }]"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD mood ON person TYPE option<mood>;
DEFINE TABLE top AS SELECT math::max(mood) AS top, count() AS n FROM person GROUP ALL;
CREATE person:1 SET mood = 'ok';
DEFINE TABLE bottom AS SELECT math::min(mood) AS bottom FROM person GROUP ALL;
DEFINE TABLE counted AS SELECT mood, count() AS n FROM person GROUP BY mood;
SELECT * FROM counted;
SELECT enum::max('mood', array::group(mood)) AS top FROM person GROUP ALL;
