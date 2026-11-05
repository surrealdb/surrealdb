/**
[env]
namespace = true
database = true

[test]
reason = "A PostgreSQL table created again under the name of a dropped table counts its SERIAL column from 1, a CREATE TABLE IF NOT EXISTS and a CREATE TABLE that fails on a table that exists leave the count of that table alone"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'a' }, { id: 2, name: 'b' }, { id: 3, name: 'c' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
error = "The table 'serial_again' already exists"

[[test.results]]
value = "[{ id: 3 }]"

[[test.results]]
value = "[{ id: 1, name: 'd' }, { id: 2, name: 'e' }, { id: 3, name: 'f' }]"
*/

CREATE TABLE serial_again (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_again (name) VALUES ('a'), ('b'), ('c');

DROP TABLE serial_again;

CREATE TABLE serial_again (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_again (name) VALUES ('d') RETURNING id;

CREATE TABLE IF NOT EXISTS serial_again (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_again (name) VALUES ('e') RETURNING id;

CREATE TABLE serial_again (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_again (name) VALUES ('f') RETURNING id;

SELECT id, name FROM serial_again ORDER BY id;
