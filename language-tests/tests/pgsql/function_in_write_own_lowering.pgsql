/**
[env]
namespace = true
database = true

[test]
reason = "A function that has a lowering of its own answers as it does in a read in the values, conditions and RETURNING of a write"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, n: 1i32, s: '7', ts: d'2019-05-01T00:00:00Z' }, { id: 2, n: 2i32, s: '42', ts: d'2021-05-01T00:00:00Z' }, { id: 3, n: 3i32, s: 'abc', ts: d'2018-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: 1, n: 1i32, s: '00007', ts: d'2019-05-01T00:00:00Z' }, { id: 2, n: 2i32, s: '00042', ts: d'2021-05-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: 1, n: 1i32, s: '00007', ts: d'2019-05-01T00:00:00Z' }, { id: 3, n: 3i32, s: 'abc', ts: d'2018-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: 2, s: '00042' }]"

[[test.results]]
value = "[{ id: 9, p: 3, u: 'BCD' }]"

[[test.results]]
value = "[{ id: 2, n: 1i32, s: '42' }]"

[[test.results]]
value = "[{ id: 9, s: '**x' }]"

[[test.results]]
value = "[{ id: 2, n: 1i32, s: '42' }, { id: 9, n: 1i32, s: '**x' }]"

[[test.results]]
value = "[{ id: 9, s: 'xxx*' }]"
*/

CREATE TABLE wr (id int PRIMARY KEY, s text, ts timestamptz, n int);

INSERT INTO wr (id, s, ts, n) VALUES (1, '7', CAST('2019-05-01T00:00:00Z' AS timestamptz), 1), (2, '42', CAST('2021-05-01T00:00:00Z' AS timestamptz), 2), (3, 'abc', CAST('2018-01-01T00:00:00Z' AS timestamptz), 3);

UPDATE wr SET s = lpad(s, 5, '0') WHERE id < 3;

DELETE FROM wr WHERE extract(year FROM ts) < 2020;

SELECT id, s FROM wr ORDER BY id;

INSERT INTO wr (id, s, n) VALUES (9, substring('abcdef' FROM 2 FOR 3), 1) RETURNING id, upper(s) AS u, strpos(s, 'd') AS p;

UPDATE wr SET s = ltrim(s, '0'), n = mod(n + 7, 4) WHERE date_part('year', ts) = 2021 RETURNING id, s, n;

INSERT INTO wr (id, s, n) VALUES (9, 'x', 1) ON CONFLICT (id) DO UPDATE SET s = lpad(excluded.s, 3, '*') WHERE strpos(wr.s, 'b') > 0 RETURNING id, s;

SELECT id, s, n FROM wr ORDER BY id;

UPDATE wr SET s = substring(lpad(upper(s), 7, 'x'), 2, 4) WHERE id = 9 RETURNING id, s;
