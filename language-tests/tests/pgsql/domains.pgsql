/**
[env]
namespace = true
database = true

[test]
reason = "CREATE DOMAIN, ALTER DOMAIN and DROP DOMAIN: a domain CHECK refusing a value, a NOT NULL domain, a domain DEFAULT, a constraint added and dropped, and a domain that a column uses refused a drop"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 'none', id: 1, qty: 5i32 }]"

[[test.results]]
error = "Found -1i32 for field `qty`, with record `item:2`, but field must conform to: (IF $value = NULL OR $value = NONE OR 0 = NULL OR 0 = NONE THEN NULL ELSE $value > 0 END) != false"

[[test.results]]
error = "An error occurred: domain code does not allow null values"

[[test.results]]
value = "[{ c: 'x', id: 4, qty: NULL }]"

[[test.results]]
value = "[{ c: 'none', id: 1, qty: 5i32 }, { c: 'x', id: 4, qty: NULL }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 500i32 for field `qty`, with record `item:5`, but field must conform to: (IF $value = NULL OR $value = NONE OR 0 = NULL OR 0 = NONE THEN NULL ELSE $value > 0 END) != false AND (IF $value = NULL OR $value = NONE OR 100 = NULL OR 100 = NONE THEN NULL ELSE $value < 100 END) != false"

[[test.results]]
value = "[{ c: 'none', id: 6, qty: 50i32 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 'none', id: 7, qty: 500i32 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 'other', id: 8, qty: 1i32 }]"

[[test.results]]
value = "[{ c: 'none', id: 6, qty: 50i32 }, { c: 'none', id: 7, qty: 500i32 }, { c: 'other', id: 8, qty: 1i32 }]"

[[test.results]]
value = "[{ c: 'x', id: 4, qty: NULL }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: domain posint does not allow null values"

[[test.results]]
value = "[{ c: 'z', id: 10, qty: 3i32 }]"

[[test.results]]
error = "Invalid statement: cannot drop type posint because other objects depend on it: column qty of table item depends on type posint"

[[test.results]]
error = 'Invalid statement: type "posint" already exists'

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = 'Invalid statement: type "code" does not exist'

*/

CREATE DOMAIN posint AS integer CHECK (VALUE > 0);

CREATE DOMAIN code AS text NOT NULL DEFAULT 'none';

CREATE TABLE item (id int PRIMARY KEY, qty posint, c code);

INSERT INTO item (id, qty) VALUES (1, 5);

INSERT INTO item (id, qty) VALUES (2, -1);

INSERT INTO item (id, qty, c) VALUES (3, 2, NULL);

INSERT INTO item (id, qty, c) VALUES (4, NULL, 'x');

SELECT id, qty, c FROM item ORDER BY id;

ALTER DOMAIN posint ADD CONSTRAINT small CHECK (VALUE < 100);

INSERT INTO item (id, qty) VALUES (5, 500);

INSERT INTO item (id, qty) VALUES (6, 50);

ALTER DOMAIN posint DROP CONSTRAINT small;

INSERT INTO item (id, qty) VALUES (7, 500);

ALTER DOMAIN code SET DEFAULT 'other';

INSERT INTO item (id, qty) VALUES (8, 1);

SELECT id, qty, c FROM item WHERE id >= 5 ORDER BY id;

DELETE FROM item WHERE id = 4;

ALTER DOMAIN posint SET NOT NULL;

INSERT INTO item (id, c) VALUES (9, 'y');

INSERT INTO item (id, qty, c) VALUES (10, 3, 'z');

DROP DOMAIN posint;

CREATE DOMAIN posint AS integer;

DROP TABLE item;

DROP DOMAIN posint;

DROP DOMAIN code;

DROP DOMAIN IF EXISTS code;

DROP DOMAIN code;
