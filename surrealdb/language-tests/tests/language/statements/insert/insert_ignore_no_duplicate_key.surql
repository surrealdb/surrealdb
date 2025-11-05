/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE INDEX name ON person FIELDS name UNIQUE;
CREATE person:tobie SET name = 'Tobie';
INSERT IGNORE INTO person { id: 1, name: 'Tobie' };
INSERT IGNORE INTO person (id, name) VALUES (2, 'Tobie');
