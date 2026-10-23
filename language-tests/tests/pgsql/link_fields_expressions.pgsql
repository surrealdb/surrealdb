/**
[env]
namespace = true
database = true
imports = ["pgsql/_link_fields.surql"]

[test]
reason = "A column declared as a link to one table is its key wherever it is an operand of an expression, so an ordering comparison or BETWEEN against an integer, float or string bound compares the key, as the id column does, and arithmetic, a cast to int or text, a concatenation, CASE, COALESCE and the aggregates sum, min and max see the integer or string key and not the record. UPDATE and DELETE read the link column of their target table the same way in a WHERE clause and in a RETURNING expression."

[[test.results]]
value = "[{ name: 'Sam' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Sam' }]"

[[test.results]]
value = "[{ name: 'Sam' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Sam' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Sam' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Bob' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Sam' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Bob' }]"

[[test.results]]
value = "[{ n: 2, name: 'Eve' }, { n: 3, name: 'Sam' }, { n: 2, name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Eve', odd: 1 }, { name: 'Sam', odd: 0 }, { name: 'Ann', odd: 1 }]"

[[test.results]]
value = "[{ n: -1, name: 'Eve' }, { n: -2, name: 'Sam' }, { n: -1, name: 'Ann' }]"

[[test.results]]
value = "[{ d: 2, name: 'Eve' }, { d: 1, name: 'Sam' }, { d: 2, name: 'Ann' }]"

[[test.results]]
value = "[{ n: 1, name: 'Eve' }, { n: 2, name: 'Sam' }, { n: 1, name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Eve', t: '1' }, { name: 'Sam', t: '2' }, { name: 'Ann', t: '1' }, { name: 'Bob', t: 'ops' }]"

[[test.results]]
value = "[{ name: 'Eve', t: '1' }, { name: 'Sam', t: '2' }, { name: 'Ann', t: '1' }, { name: 'Bob', t: 'ops' }]"

[[test.results]]
value = "[{ name: 'Eve', s: '1!' }, { name: 'Sam', s: '2!' }, { name: 'Ann', s: '1!' }, { name: 'Bob', s: 'ops!' }]"

[[test.results]]
value = "[{ c: 'one', name: 'Eve' }, { c: 'two', name: 'Sam' }, { c: 'one', name: 'Ann' }, { c: 'other', name: 'Bob' }]"

[[test.results]]
value = "[{ c: 'lo', name: 'Eve' }, { c: 'hi', name: 'Sam' }, { c: 'lo', name: 'Ann' }]"

[[test.results]]
value = "[{ m: 0, name: 'Eve' }, { m: 0, name: 'Sam' }, { m: 1, name: 'Ann' }]"

[[test.results]]
value = "[{ hi: 2, lo: 1, s: 4 }]"

[[test.results]]
value = "[{ id: 4 }]"

[[test.results]]
value = "[{ id: 2, note: 'gt1' }]"

[[test.results]]
value = "[{ id: 1, note: 'lt2' }, { id: 3, note: 'lt2' }]"

[[test.results]]
value = "[{ id: 2, note: 'ge' }]"

[[test.results]]
value = "[{ id: 2, note: 'btw' }]"

[[test.results]]
value = "[{ id: 1, note: 'nbtw' }, { id: 3, note: 'nbtw' }]"

[[test.results]]
value = "[{ bumped: 2, id: 1 }, { bumped: 2, id: 3 }]"

[[test.results]]
value = "[{ id: 2, note: 'abs' }]"

[[test.results]]
value = "[{ dept: dept:2, id: 5 }, { dept: dept:3, id: 6 }, { dept: dept:4, id: 7 }]"

[[test.results]]
value = "[{ id: 7 }]"

[[test.results]]
value = "[{ id: 5 }]"

[[test.results]]
value = "[{ id: 6 }]"

[[test.results]]
value = "[{ bumped: 3, id: 2 }]"

[[test.results]]
value = "[{ id: 1, name: 'Eve', note: 'plus' }, { id: 3, name: 'Ann', note: 'plus' }]"
*/

SELECT name FROM emp WHERE dept > 1 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept < 2 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE 1 < dept AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept >= 1.5 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept <= 1.5 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept BETWEEN 1 AND 3 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept BETWEEN 2 AND 3 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept NOT BETWEEN 2 AND 3 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept BETWEEN 0.5 AND 1.5 AND id < 4 ORDER BY id;

SELECT name FROM emp WHERE dept > 'a' ORDER BY id;

SELECT name FROM emp WHERE dept < 'a' ORDER BY id;

SELECT name FROM emp WHERE dept BETWEEN 'a' AND 'z' ORDER BY id;

SELECT name, dept + 1 AS n FROM emp WHERE id < 4 ORDER BY id;

SELECT name, dept % 2 AS odd FROM emp WHERE id < 4 ORDER BY id;

SELECT name, -dept AS n FROM emp WHERE id < 4 ORDER BY id;

SELECT name, abs(dept - 3) AS d FROM emp WHERE id < 4 ORDER BY id;

SELECT name, CAST(dept AS int) AS n FROM emp WHERE id < 4 ORDER BY id;

SELECT name, CAST(dept AS text) AS t FROM emp ORDER BY id;

SELECT name, dept::text AS t FROM emp ORDER BY id;

SELECT name, dept || '!' AS s FROM emp ORDER BY id;

SELECT name, CASE dept WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS c FROM emp ORDER BY id;

SELECT name, CASE WHEN dept > 1 THEN 'hi' ELSE 'lo' END AS c FROM emp WHERE id < 4 ORDER BY id;

SELECT name, COALESCE(mentor, 0) AS m FROM emp WHERE id < 4 ORDER BY id;

SELECT sum(dept) AS s, min(dept) AS lo, max(dept) AS hi FROM emp WHERE id < 4;

DELETE FROM emp WHERE name = 'Bob' RETURNING id;

UPDATE emp SET note = 'gt1' WHERE dept > 1 RETURNING id, note;

UPDATE emp SET note = 'lt2' WHERE dept < 2 RETURNING id, note;

UPDATE emp SET note = 'ge' WHERE dept >= 1.5 RETURNING id, note;

UPDATE emp SET note = 'btw' WHERE dept BETWEEN 2 AND 3 RETURNING id, note;

UPDATE emp SET note = 'nbtw' WHERE dept NOT BETWEEN 2 AND 3 RETURNING id, note;

UPDATE emp SET note = 'plus' WHERE dept + 1 = 2 RETURNING id, dept + 1 AS bumped;

UPDATE emp SET note = 'abs' WHERE abs(dept - 3) = 1 RETURNING id, note;

INSERT INTO emp (id, name, dept) VALUES (5, 'Kim', 2), (6, 'Lou', 3), (7, 'Max', 4) RETURNING id, dept;

DELETE FROM emp WHERE dept > 3 RETURNING id;

DELETE FROM emp WHERE dept < 3 AND id > 4 RETURNING id;

DELETE FROM emp WHERE dept BETWEEN 3 AND 3 RETURNING id;

DELETE FROM emp WHERE dept * 2 = 4 RETURNING id, dept + 1 AS bumped;

SELECT id, name, note FROM emp ORDER BY id;
