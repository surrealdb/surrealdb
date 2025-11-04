/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:tobias, name: 'Tobias' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE INDEX name ON person FIELDS name UNIQUE;
CREATE person:tobie SET name = 'Tobie';
CREATE person:tobias SET name = 'Tobias';
INSERT IGNORE INTO person { id: 1, name: 'Tobie' } ON DUPLICATE KEY UPDATE name = 'Tobias';
INSERT IGNORE INTO person (id, name) VALUES (2, 'Tobie') ON DUPLICATE KEY UPDATE name = 'Tobias';
