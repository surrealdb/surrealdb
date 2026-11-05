/**
[env]
namespace = true
database = true

[test]
reason = "TRUNCATE ... RESTART IDENTITY starts the sequences of every SERIAL column of the table over and no other table's, whichever of the optional words follow it, and needs no SERIAL column"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, n: 1 }, { id: 2, n: 2 }, { id: 3, n: 3 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1, n: 1 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 2, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

CREATE TABLE trunc_ri (id SERIAL PRIMARY KEY, n BIGSERIAL, name TEXT);

CREATE TABLE trunc_ri_other (id SERIAL PRIMARY KEY);

INSERT INTO trunc_ri (name) VALUES ('a'), ('b'), ('c') RETURNING id, n;

INSERT INTO trunc_ri_other DEFAULT VALUES RETURNING id;

TRUNCATE TABLE trunc_ri RESTART IDENTITY;

INSERT INTO trunc_ri (name) VALUES ('d') RETURNING id, n;

INSERT INTO trunc_ri_other DEFAULT VALUES RETURNING id;

INSERT INTO trunc_ri (name) VALUES ('e') RETURNING id, n;

TRUNCATE TABLE ONLY trunc_ri RESTART IDENTITY CASCADE;

INSERT INTO trunc_ri (name) VALUES ('f') RETURNING id, n;

CREATE TABLE trunc_ri_plain (a INT);

TRUNCATE trunc_ri_plain RESTART IDENTITY;
