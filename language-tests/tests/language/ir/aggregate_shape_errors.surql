/**
[env]
namespace = true
database = true

[test]
reason = """
An expression that fails inside an aggregate's argument evaluates to NONE for that row:
the aggregate counts or sums the remaining rows. The same failure raises in a plain projection, a
GROUP BY key that fails groups under NONE, and a failed cast raises even inside an aggregate.
"""

[[test.results]]
value = "[{ c: 0 }]"

[[test.results]]
value = "[{ s: 2 }]"

[[test.results]]
error = "Cannot perform addition with 'int' and 'string'"

[[test.results]]
value = "[{ c: 1, k: NONE }]"

[[test.results]]
error = "Could not cast into `int` using input `'z'`"
*/

SELECT count(a + 'x') AS c FROM [{ a: 1 }, { a: 2 }] GROUP ALL;
SELECT math::sum(a + 1) AS s FROM [{ a: 1 }, { a: 'x' }] GROUP ALL;
SELECT VALUE a + 'x' FROM [{ a: 1 }];
SELECT (a + 'x') AS k, count() AS c FROM [{ a: 1 }] GROUP BY k;
SELECT count(<int>'z') AS c FROM [{ a: 1 }] GROUP ALL;
