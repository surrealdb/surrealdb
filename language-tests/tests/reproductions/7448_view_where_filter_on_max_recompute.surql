/**
[env]
timeout = 5000

[test]
reason = "A view's WHERE filter applies when math::max is recomputed after an UPDATE"
issue = 7448

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'k', id: person:n0, x: 1 }]"

[[test.results]]
value = "[{ g: 'k', id: person:n1 }]"

[[test.results]]
value = "[{ g: 'k', id: person:n2, x: 996 }]"

[[test.results]]
value = "[{ g: 'k', id: person:n2, x: 45 }]"

[[test.results]]
value = "[{ a: 45, g: 'k' }]"

[[test.results]]
value = "[{ g: 'k', id: person:n0, x: 60 }]"

[[test.results]]
value = "[{ g: 'k', id: person:n0, x: 2 }]"

[[test.results]]
value = "[{ a: 45, g: 'k' }]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE v AS SELECT g, math::max(x) AS a FROM person WHERE x != NONE GROUP BY g;
CREATE person:n0 SET g = 'k', x = 1;
CREATE person:n1 SET g = 'k';
CREATE person:n2 SET g = 'k', x = 996;
UPDATE person:n2 SET x = 45;
SELECT g, a FROM v;
UPDATE person:n0 SET x = 60;
UPDATE person:n0 SET x = 2;
SELECT g, a FROM v;
