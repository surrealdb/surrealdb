/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An `IN` subquery lifted into a join is evaluated only when a left row
	reaches it. With no left row satisfying the other conjuncts, a right
	side whose filter raises on a stored value is never built, as the legacy
	engine never evaluates the subquery for a row `AND` has already refused.
	With a left row that reaches it, both engines raise the cast error.
"""

[[test.results]]
value = "[{ id: t:1, x: 1 }]"

[[test.results]]
value = "[{ c: 'bad', id: u:1, k: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'bad'`"
*/

CREATE t:1 SET x = 1;
CREATE u:1 SET k = 1, c = 'bad';
SELECT id FROM t WHERE x = 99 AND x IN (SELECT VALUE k FROM u WHERE <int> c > 0);
SELECT id FROM t WHERE x = 1 AND x IN (SELECT VALUE k FROM u WHERE <int> c > 0);
