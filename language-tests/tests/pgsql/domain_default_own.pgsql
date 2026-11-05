/**
[env]
namespace = true
database = true

[test]
reason = "A column that names its own DEFAULT keeps it when its domain's default changes, even one equal to the domain's; a column that names none follows the domain"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, inherited: 'none', own: 'none' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 2, inherited: 'other', own: 'none' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 3, inherited: NULL, own: 'none' }]"

[[test.results]]
value = "[{ id: 1, inherited: 'none', own: 'none' }, { id: 2, inherited: 'other', own: 'none' }, { id: 3, inherited: NULL, own: 'none' }]"

*/

CREATE DOMAIN code AS text DEFAULT 'none';

CREATE TABLE item (id int PRIMARY KEY, own code DEFAULT 'none', inherited code);

INSERT INTO item (id) VALUES (1);

ALTER DOMAIN code SET DEFAULT 'other';

INSERT INTO item (id) VALUES (2);

ALTER DOMAIN code DROP DEFAULT;

INSERT INTO item (id) VALUES (3);

SELECT id, own, inherited FROM item ORDER BY id;
