/**
[env]
namespace = true
database = true

[test]
reason = "CREATE TYPE ... AS (...) and DROP TYPE of a composite type: a composite column written with ROW(...)::type, read whole and by (col).field, and a type that a column uses refused a drop"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ home: { street: 'Main St', zip: 12345i32 }, id: 1, name: 'a' }]"

[[test.results]]
value = "[{ home: { street: 'Side Rd', zip: 54321i32 }, id: 2, name: 'b' }]"

[[test.results]]
value = "[{ id: 1, street: 'Main St', zip: 12345i32 }, { id: 2, street: 'Side Rd', zip: 54321i32 }]"

[[test.results]]
value = "[{ name: 'b' }]"

[[test.results]]
value = "[{ a: { street: 'Elm', zip: 7 } }]"

[[test.results]]
value = "[{ s: 'Elm' }]"

[[test.results]]
error = "Invalid statement: Cannot remove the type 'address': the field 'home' on the table 'person' has a type that names it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

CREATE TYPE address AS (street text, zip int);

CREATE TABLE person (id int PRIMARY KEY, name text, home address);

INSERT INTO person (id, name, home) VALUES (1, 'a', ROW('Main St', 12345)::address);

INSERT INTO person (id, name, home) VALUES (2, 'b', ('Side Rd', 54321)::address);

SELECT id, (home).street AS street, (home).zip FROM person ORDER BY id;

SELECT name FROM person WHERE (home).zip = 54321;

SELECT ROW('Elm', 7)::address AS a;

SELECT (ROW('Elm', 7)::address).street AS s;

DROP TYPE address;

DROP TABLE person;

DROP TYPE address;

DROP TYPE IF EXISTS address;
