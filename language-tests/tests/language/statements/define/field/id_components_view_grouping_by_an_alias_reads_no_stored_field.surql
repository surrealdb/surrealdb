/**
[env]
namespace = true
database = true

[test]
reason = """
	A view that groups by the alias of a projected expression, as in
	`SELECT id[0] AS city ... GROUP BY city`, groups by that expression: it
	reads the record id, not a stored field named `city`. Naming `city` in the
	record id leaves what the view reads unchanged, so the statement is accepted
	and the view keeps maintaining its rows from the keys. A view whose
	condition reads a source field under the name is still refused.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: reading:['London', d'2026-01-01T00:00:00Z'], v: 1 }, { id: reading:['London', d'2026-01-01T00:00:30.500Z'], v: 2 }, { id: reading:['Paris', d'2026-01-01T00:01:00Z'], v: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ city: 'London', time: d'2026-01-01T00:00:45Z', v: 4 }]"

[[test.results]]
value = "[{ city: 'London', id: by_minute:['London', d'2026-01-01T00:00:00Z'], minute: d'2026-01-01T00:00:00Z', n: 3, total: 7 }, { city: 'Paris', id: by_minute:['Paris', d'2026-01-01T00:01:00Z'], minute: d'2026-01-01T00:01:00Z', n: 1, total: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot name `city` in the record id of `tick`: the view `london_ticks` already reads a stored `city` field. Remove it, then define it again after changing the record id"
*/

DEFINE TABLE reading;
INSERT INTO reading [
	{ id: reading:['London', d'2026-01-01T00:00:00Z'], v: 1 },
	{ id: reading:['London', d'2026-01-01T00:00:30.5Z'], v: 2 },
	{ id: reading:['Paris', d'2026-01-01T00:01:00Z'], v: 3 },
];
DEFINE TABLE by_minute AS SELECT id[0] AS city, time::floor(id[1], 1m) AS minute, count() AS n, math::sum(v) AS total FROM reading GROUP BY city, minute;
DEFINE TABLE everything AS SELECT count() AS n FROM reading GROUP ALL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
CREATE reading SET city = 'London', time = d'2026-01-01T00:00:45Z', v = 4 RETURN city, time, v;
SELECT * FROM by_minute ORDER BY id;

-- A view whose condition reads the name reads the stored field, alias or not.
DEFINE TABLE tick;
DEFINE TABLE london_ticks AS SELECT id[0] AS place, count() AS n FROM tick WHERE city = 'London' GROUP BY place;
DEFINE FIELD id ON tick TYPE [city: string, time: datetime];
