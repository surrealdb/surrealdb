/**
[env]
namespace = true
database = true

[test]
reason = "a numeric constant is read as an exact numeric, normalised as a SurrealQL decimal is; a cast to numeric(p,s) rounds half away from zero to the scale, of a constant and of a value per row alike"

[[test.results]]
value = "[{ exact: true, exponent: 1000dec, kept: 1.5dec, negative: -0.5dec, product: 5dec }]"

[[test.results]]
value = "[{ exponent: 'numeric', fraction: 'numeric', wide: 'numeric' }]"

[[test.results]]
value = "[{ big: 123456789012345678901234567891.123bigdec }]"

[[test.results]]
value = "[{ away: -3dec, i: 2, j: 3, narrowed: 123.5dec, rounded: 1.26dec }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, s: '7.125', x: 2.25f }, { id: 2, s: NULL, x: NULL }]"

[[test.results]]
value = "[{ id: 1, s: 7.13dec, x: 2.3dec }, { id: 2, s: NULL, x: NULL }]"

[[test.results]]
error = "numeric field overflow (cast to numeric(5,2))"
*/

SELECT 0.1 + 0.2 = 0.3 AS exact, 1.50 AS kept, 2.5 * 2 AS product, 1e3 AS exponent, -0.5 AS negative;

SELECT pg_typeof(1.5) AS fraction, pg_typeof(1e3) AS exponent, pg_typeof(10000000000000000000) AS wide;

SELECT 123456789012345678901234567890.123 + 1 AS big;

SELECT CAST(1.255 AS numeric(10,2)) AS rounded, 123.456::numeric(5,1) AS narrowed, (-2.5)::numeric(2,0) AS away, 1.5::int AS i, 2.5::int AS j;

CREATE TABLE p (id int PRIMARY KEY, x float8, s text);

INSERT INTO p (id, x, s) VALUES (1, 2.25, '7.125'), (2, NULL, NULL);

SELECT id, CAST(x AS numeric(4,1)) AS x, s::numeric(6,2) AS s FROM p ORDER BY id;

SELECT 12345.6::numeric(5,2);
