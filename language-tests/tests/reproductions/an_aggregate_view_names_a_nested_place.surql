/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An aggregated view's field name is a path, and the row a group produces is
built by walking it. The backfill a `DEFINE TABLE` runs over the rows that
already exist and the maintenance a later write runs both address that path,
so a nested name must produce the same row whichever of them built it.
"""

[[test.results]]
value = "[{ g: 'a', id: t:1, n: 1 }]"

[[test.results]]
value = "[{ g: 'a', id: t:2, n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', stats: { rows: 2, total: 3 } }]"

[[test.results]]
value = "[{ g: 'b', id: t:3, n: 4 }]"

[[test.results]]
value = "[{ g: 'a', stats: { rows: 2, total: 3 } }, { g: 'b', stats: { rows: 1, total: 4f } }]"

[[test.results]]
value = "[{ g: 'a', id: t:4, n: 3 }]"

[[test.results]]
value = "[{ g: 'a', stats: { rows: 3, total: 6 } }, { g: 'b', stats: { rows: 1, total: 4f } }]"
*/

-- Rows that exist before the view does, so their group row comes from the backfill.
CREATE t:1 SET g = "a", n = 1;
CREATE t:2 SET g = "a", n = 2;

DEFINE TABLE v AS
	SELECT g, count() AS stats.rows, math::sum(n) AS stats.total
	FROM t GROUP BY g;

SELECT g, stats FROM v ORDER BY g;

-- A row written after the view exists, so its group row comes from maintenance.
CREATE t:3 SET g = "b", n = 4;

SELECT g, stats FROM v ORDER BY g;

-- And a group the backfill already built, extended by maintenance.
CREATE t:4 SET g = "a", n = 3;

SELECT g, stats FROM v ORDER BY g;
