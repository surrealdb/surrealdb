/**
[env]
namespace = true
database = true

[test]
reason = "A CREATE TABLE IF NOT EXISTS that creates the table, because a dropped table of the name is gone, counts its SERIAL column from 1; one that finds the table leaves the count alone"

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
*/

CREATE TABLE serial_ine (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_ine (name) VALUES ('a'), ('b'), ('c');

DROP TABLE serial_ine;

CREATE TABLE IF NOT EXISTS serial_ine (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_ine (name) VALUES ('d') RETURNING id;

CREATE TABLE IF NOT EXISTS serial_ine (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_ine (name) VALUES ('e') RETURNING id;
