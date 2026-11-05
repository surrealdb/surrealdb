/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A cast or a field read a kind proof shows cannot fail or fetch lets a predicate move beneath a projection, and every other cast and read keeps its error and its rows"

[[test.results]]
value = "[{ a: 2, id: t:1 }]"

[[test.results]]
value = "[{ a: 'x', id: t:2 }]"

[[test.results]]
value = "[{ id: t:3 }]"

[[test.results]]
value = "[{ id: t:1, p: true }, { id: t:2, p: true }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ id: t:1, p: 2 }, { id: t:2, p: 'x' }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `number` using input `true`"
*/

CREATE t:1 SET a = 2;
CREATE t:2 SET a = 'x';
CREATE t:3;

-- A cast of a proven `bool` to `option<bool>` is the comparison itself.
SELECT * FROM (SELECT id, <option<bool>> (a > 1) AS p FROM t) WHERE p ORDER BY id;

-- A cast of a field is not proven: it fails on the second row.
SELECT * FROM (SELECT id, <int> a AS p FROM t) WHERE p ORDER BY id;

-- A read of a field of an object literal fetches nothing.
SELECT * FROM (SELECT id, { x: a }.x AS p FROM t) WHERE p ORDER BY id;

-- A read of a field of a stored value is not proven.
SELECT * FROM (SELECT id, a.x AS p FROM t) WHERE p ORDER BY id;

-- Arithmetic over a cast keeps the cast's failure.
SELECT * FROM (SELECT id, (<int> a) + 1 AS p FROM t) WHERE p ORDER BY id;

-- A cast that is not an identity converts.
SELECT * FROM (SELECT id, <number> (a > 1) AS p FROM t) WHERE p ORDER BY id;
