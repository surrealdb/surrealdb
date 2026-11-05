/**
[env]
namespace = true
database = true
imports = ["pgsql/_omitted_column.surql"]

[test]
reason = "A column an INSERT leaves out holds NULL, as one the INSERT gives NULL does, so that the rows are one value to GROUP BY and UNION; a column with a DEFAULT takes the default"

[[test.results]]
value = "[{ a: 10, b: NULL, d: 7, id: 1 }]"

[[test.results]]
value = "[{ a: 20, b: NULL, d: 7, id: 2 }]"

[[test.results]]
value = "[{ a: 30, b: 'x', d: 7, id: 3 }]"

[[test.results]]
value = "[{ b: 'x' }, { b: NULL }]"

[[test.results]]
value = "[{ b: 'x' }, { b: NULL }]"

[[test.results]]
value = "[{ n: 2 }]"
*/

INSERT INTO omit_t (id, a) VALUES (1, 10) RETURNING id, a, b, d;

INSERT INTO omit_t (id, a, b) VALUES (2, 20, NULL) RETURNING id, a, b, d;

INSERT INTO omit_t (id, a, b) VALUES (3, 30, 'x') RETURNING id, a, b, d;

SELECT b FROM omit_t GROUP BY b ORDER BY b;

SELECT b FROM omit_t UNION SELECT b FROM omit_t ORDER BY b;

SELECT count(*) AS n FROM omit_t WHERE b IS NULL;
