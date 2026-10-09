/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An aggregate's argument and a group key are evaluated in a region of their own: a product the projection computes is not read by an aggregate's argument, and a cast that fails in an aggregate raises with its message."

[[test.results]]
value = "[{ a: 1, id: t:1, v: '1' }]"

[[test.results]]
value = "[{ a: 2, id: t:2, v: '2' }]"

[[test.results]]
value = "[{ a: 1, id: t:3, v: '3' }]"

[[test.results]]
value = "[{ k: 2, n: 2, s: 4 }, { k: 4, n: 1, s: 4 }]"

[[test.results]]
value = "[{ k: 2, n: 1 }, { k: 4, n: 1 }]"

[[test.results]]
value = "[{ s: 6 }]"

[[test.results]]
value = "[{ a: 3, id: t:4, v: 'x' }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

CREATE t:1 SET a = 1, v = '1';
CREATE t:2 SET a = 2, v = '2';
CREATE t:3 SET a = 1, v = '3';

-- The key and the aggregate's argument are the same text, evaluated apart.
SELECT a * 2 AS k, math::sum(a * 2) AS s, count() AS n FROM t GROUP BY k ORDER BY k;
SELECT a * 2 AS k, array::len(array::group(a * 2)) AS n FROM t GROUP BY k ORDER BY k;

-- A cast in an aggregate's argument.
SELECT math::sum(<int> v) AS s FROM t GROUP ALL;
CREATE t:4 SET a = 3, v = 'x';
SELECT math::sum(<int> v) AS s FROM t GROUP ALL;
