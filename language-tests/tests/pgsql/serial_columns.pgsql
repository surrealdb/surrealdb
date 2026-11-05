/**
[env]
namespace = true
database = true

[test]
reason = "A PostgreSQL SERIAL or BIGSERIAL column takes the next number of a sequence of its own when an insert leaves it out or gives DEFAULT, keeps a number an insert gives, and counts apart from the other generated column of its table"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 1i32, ticket: 1 }, { n: 2i32, ticket: 2 }]"

[[test.results]]
value = "[{ n: 3i32, ticket: 3 }]"

[[test.results]]
value = "[{ n: 10i32, ticket: 4 }]"

[[test.results]]
value = "[{ n: 4i32, ticket: 5 }]"

[[test.results]]
value = "[{ n: 1i32, name: 'a', ticket: 1 }, { n: 2i32, name: 'b', ticket: 2 }, { n: 3i32, name: 'c', ticket: 3 }, { n: 4i32, name: 'default', ticket: 5 }, { n: 10i32, name: 'given', ticket: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'x' }, { id: 2, name: 'y' }]"

[[test.results]]
value = "[{ id: 3 }]"
*/

CREATE TABLE serial_items (n SERIAL PRIMARY KEY, ticket BIGSERIAL, name TEXT);

INSERT INTO serial_items (name) VALUES ('a'), ('b') RETURNING n, ticket;

INSERT INTO serial_items (name) VALUES ('c') RETURNING n, ticket;

INSERT INTO serial_items (n, name) VALUES (10, 'given') RETURNING n, ticket;

INSERT INTO serial_items (n, name) VALUES (DEFAULT, 'default') RETURNING n, ticket;

SELECT n, name, ticket FROM serial_items ORDER BY n;

CREATE TABLE serial_keyed (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO serial_keyed (name) VALUES ('x'), ('y') RETURNING id, name;

INSERT INTO serial_keyed (name) VALUES ('z') RETURNING id;
