/**
[env]
namespace = true
database = true

[test]
reason = "TRUNCATE ... RESTART IDENTITY of a table starts over the sequences of that table only, also when another table's name is that table's name followed by a dot: trunc_ds and trunc_ds.t (a table of the schema trunc_ds) are counted apart"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 2 }]"
*/

CREATE SCHEMA trunc_ds;

CREATE TABLE trunc_ds (id SERIAL PRIMARY KEY, name TEXT);

CREATE TABLE trunc_ds.t (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO trunc_ds (name) VALUES ('a'), ('b') RETURNING id;

INSERT INTO trunc_ds.t (name) VALUES ('a'), ('b') RETURNING id;

TRUNCATE TABLE trunc_ds RESTART IDENTITY;

INSERT INTO trunc_ds (name) VALUES ('c') RETURNING id;

INSERT INTO trunc_ds.t (name) VALUES ('c') RETURNING id;

TRUNCATE TABLE trunc_ds.t RESTART IDENTITY;

INSERT INTO trunc_ds.t (name) VALUES ('d') RETURNING id;

INSERT INTO trunc_ds (name) VALUES ('d') RETURNING id;
