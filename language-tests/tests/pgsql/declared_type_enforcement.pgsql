/**
[env]
namespace = true
database = true

[test]
reason = "A column's declared type is enforced on every SQL write: an over-long varchar is refused with PostgreSQL's message, a char(n) drops trailing spaces beyond n, numeric(5,2) rounds half away from zero and refuses an overflow, and timestamp(0) rounds to whole seconds"

[[test.results]]
value = "NONE"

[[test.results]]
error = "value too long for type character varying(3) (field `v` of `dt:1`, SQL TYPE varchar(3))"

[[test.results]]
value = "[{ c: 'ab', id: 1, n: 123.46dec, ts: ts'2026-10-27T12:00:01', v: 'abc' }]"

[[test.results]]
value = "[{ c: 'ab', id: 1, n: 123.46dec, ts: ts'2026-10-27T12:00:01', v: 'abc' }]"

[[test.results]]
error = "numeric field overflow (field `n` of `dt:2`, SQL TYPE numeric(5,2))"

[[test.results]]
error = "numeric field overflow (field `n` of `dt:2`, SQL TYPE numeric(5,2))"

[[test.results]]
value = "[{ c: NULL, id: 2, n: -999.99dec, ts: NULL, v: NULL }]"

[[test.results]]
error = "value too long for type character varying(3) (field `v` of `dt:1`, SQL TYPE varchar(3))"

[[test.results]]
error = "value too long for type character(4) (field `c` of `dt:1`, SQL TYPE char(4))"

[[test.results]]
error = "numeric field overflow (field `n` of `dt:1`, SQL TYPE numeric(5,2))"

[[test.results]]
value = "[{ c: 'ab', id: 1, n: 123.46dec, v: 'abc' }, { c: NULL, id: 2, n: -999.99dec, v: NULL }]"
*/

CREATE TABLE dt (id INT PRIMARY KEY, v VARCHAR(3), c CHAR(4), n NUMERIC(5,2), ts TIMESTAMP(0));
INSERT INTO dt (id, v) VALUES (1, 'abcd');
INSERT INTO dt (id, v, c, n, ts) VALUES (1, 'abc', 'ab      ', 123.455, CAST('2026-10-27 12:00:00.5' AS timestamp));
SELECT id, v, c, n, ts FROM dt;
INSERT INTO dt (id, n) VALUES (2, 1000.00);
INSERT INTO dt (id, n) VALUES (2, 999.995);
INSERT INTO dt (id, n) VALUES (2, -999.994);
UPDATE dt SET v = 'abcd' WHERE id = 1;
UPDATE dt SET c = 'abcde' WHERE id = 1;
UPDATE dt SET n = 99999.5 WHERE id = 1;
SELECT id, v, c, n FROM dt ORDER BY id;
