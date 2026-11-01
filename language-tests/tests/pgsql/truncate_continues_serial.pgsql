/**
[env]
namespace = true
database = true

[test]
reason = "TRUNCATE, with or without CONTINUE IDENTITY, removes every row and leaves the sequences of the SERIAL columns of the table running, as PostgreSQL does"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, n: 1 }, { id: 2, n: 2 }, { id: 3, n: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: 0 }]"

[[test.results]]
value = "[{ id: 4, n: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: 5, n: 5 }]"
*/

CREATE TABLE trunc_serial (id SERIAL PRIMARY KEY, n BIGSERIAL, name TEXT);

INSERT INTO trunc_serial (name) VALUES ('a'), ('b'), ('c') RETURNING id, n;

TRUNCATE TABLE trunc_serial;

SELECT count(*) AS total FROM trunc_serial;

INSERT INTO trunc_serial (name) VALUES ('d') RETURNING id, n;

TRUNCATE trunc_serial CONTINUE IDENTITY;

INSERT INTO trunc_serial (name) VALUES ('e') RETURNING id, n;
