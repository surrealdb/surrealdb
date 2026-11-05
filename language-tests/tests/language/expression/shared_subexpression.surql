/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A pure subexpression a projection repeats is evaluated once per row, with the rows, the errors and the error order of evaluating each occurrence on its own, and a generator's draws are never shared"

[[test.results]]
value = "[{ a: -1, id: t:1, v: 'x' }]"

[[test.results]]
value = "[{ a: 2, id: t:2, v: 'y' }]"

[[test.results]]
value = "[{ x: -2, y: -1 }, { x: 4, y: 5 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ x: 0, y: -2 }, { x: 4, y: 4 }]"
*/

CREATE t:1 SET a = -1, v = 'x';
CREATE t:2 SET a = 2, v = 'y';

-- A repeated product and the sum that reads it.
SELECT a * 2 AS x, a * 2 + 1 AS y FROM t;

-- The cast fails on the first row, at the first field, as it does unshared.
SELECT <int> v AS x, <int> v + 1 AS y FROM t;

-- Two draws are two values.
LET $draws = SELECT rand::uuid() AS a, rand::uuid() AS b FROM ONLY t:1;
RETURN $draws.a != $draws.b;

-- An occurrence in a branch the first row skips is read where it is written.
SELECT (IF a > 0 THEN a * 2 ELSE 0 END) AS x, a * 2 AS y FROM t;
