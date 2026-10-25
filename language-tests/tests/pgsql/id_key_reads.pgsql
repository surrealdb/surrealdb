/**
[env]
namespace = true
database = true

[test]
reason = "The id column of a PostgreSQL table is its primary key: a table built and filled through SQL reads back the integer it was given, as a column, qualified, in the echo of the insert and through *, while the record id behind it stays the storage."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 2, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: 3, name: 'Zed' }]"

[[test.results]]
value = "[{ id: 4, name: 'Dan' }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 2, name: 'Jaime' }, { id: 3, name: 'Zed' }, { id: 4, name: 'Dan' }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]"

[[test.results]]
value = "[{ name: 'Tobie', pk: 1 }, { name: 'Jaime', pk: 2 }, { name: 'Zed', pk: 3 }, { name: 'Dan', pk: 4 }]"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }, { id: 2, name: 'Jaime' }, { id: 3, name: 'Zed' }, { id: 4, name: 'Dan' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 'rust', label: 'Rust' }, { id: 'db', label: 'Database' }]"

[[test.results]]
value = "[{ id: 'db', label: 'Database' }, { id: 'rust', label: 'Rust' }]"

[[test.results]]
value = "[{ id: 'db', label: 'Database' }, { id: 'rust', label: 'Rust' }]"

[[test.results]]
value = "[{ next: 2, twice: 2, txt: '1' }, { next: 3, twice: 4, txt: '2' }, { next: 4, twice: 6, txt: '3' }, { next: 5, twice: 8, txt: '4' }]"

[[test.results]]
value = "[{ shout: 'DB' }, { shout: 'RUST' }]"

[[test.results]]
value = "[{ bottom: 1, n: 4, top: 4, total: 10 }]"

[[test.results]]
value = "[{ n: 4 }]"

*/

CREATE TABLE person (id INT PRIMARY KEY, name VARCHAR(50));

INSERT INTO person (id, name) VALUES (1, 'Tobie'), (2, 'Jaime');

INSERT INTO person (id, name) VALUES (3, 'Zed') RETURNING id, name;

INSERT INTO person (id, name) VALUES (4, 'Dan') RETURNING *;

SELECT id, name FROM person ORDER BY id;

SELECT p.id FROM person p ORDER BY p.id;

SELECT person.id AS pk, name FROM person ORDER BY pk;

SELECT * FROM person ORDER BY id;

CREATE TABLE tag (id TEXT PRIMARY KEY, label TEXT);

INSERT INTO tag (id, label) VALUES ('rust', 'Rust'), ('db', 'Database');

SELECT id, label FROM tag ORDER BY id;

SELECT * FROM tag ORDER BY id;

SELECT id + 1 AS next, id * 2 AS twice, CAST(id AS TEXT) AS txt FROM person ORDER BY id;

SELECT upper(id) AS shout FROM tag ORDER BY id;

SELECT count(id) AS n, max(id) AS top, min(id) AS bottom, sum(id) AS total FROM person;

SELECT count(DISTINCT id) AS n FROM person;
