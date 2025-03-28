/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: like:1, in: person:1, out: person:2 }]"

[[test.results]]
value = "[]"

*/

DEFINE INDEX unique ON like FIELDS in, out UNIQUE;
CREATE person:1 SET name = 'Tobie';
CREATE person:2 SET name = 'Tobie';
INSERT RELATION INTO like (id, in, out) VALUES (1, person:1, person:2);
INSERT RELATION IGNORE INTO like (id, in, out) VALUES (2, person:1, person:2);
