/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 30000

[test]
reason = """
	Differential reads over aggregate views, whose rows are keyed by their
	group values: a predicate on a projected group alias may read a key
	range. Every statement returns true when the default plan returns what
	the same query WITH NOINDEX returns. The views group by a string and a
	datetime with sub-second values (a datetime key orders by the whole
	second only), by an alias of a source field, by a function of a field,
	by one field projected under two aliases, by an int (numbers are never
	pushed), and by a field next to an aggregate whose alias is another
	field's name. The comparisons run before and after the source rows are
	updated and deleted, so the maintained rows are compared too. Rows are
	ordered by their group fields, not by `id`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE v1 AS SELECT g1, g2, count() AS c FROM src GROUP BY g1, g2;
DEFINE TABLE v2 AS SELECT g1 AS a, g2 AS b, math::sum(x) AS s FROM src GROUP BY a, b;
DEFINE TABLE v3 AS SELECT g3, g1, count() AS c FROM src GROUP BY g3, g1;
DEFINE TABLE v5 AS SELECT g1, g1 AS h, count() AS c FROM src GROUP BY g1;
DEFINE TABLE v8 AS SELECT time::floor(g2, 1s) AS sec, g1, count() AS c FROM src GROUP BY sec, g1;
DEFINE TABLE v10 AS SELECT g1, count() AS g2, count() AS c FROM src GROUP BY g1;
FOR $i IN 0..90 {
	CREATE src SET
		g1 = ['a', 'b', '', 'é', 'a\0'][$i % 5],
		g2 = time::from_nanos(1704067200000000000 + ($i % 7) * 400000000 + ($i % 2)),
		g3 = ($i % 4) - 2,
		x = $i
		RETURN NONE;
};
RETURN count(SELECT * FROM v1) > 20;

RETURN (SELECT * FROM v1 WHERE g1 = 'a' ORDER BY g1, g2) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 = 'a' ORDER BY g1, g2);
RETURN (SELECT * FROM v1 WHERE g1 IN ['a', 'a\0', 'z'] AND g2 >= d'2024-01-01T00:00:01Z' ORDER BY g2 DESC, g1) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 IN ['a', 'a\0', 'z'] AND g2 >= d'2024-01-01T00:00:01Z' ORDER BY g2 DESC, g1);
RETURN (SELECT * FROM v1 WHERE g1 = 'b' AND g2 > d'2024-01-01T00:00:00.4Z' AND g2 < d'2024-01-01T00:00:02.000000001Z' ORDER BY g1, g2) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 = 'b' AND g2 > d'2024-01-01T00:00:00.4Z' AND g2 < d'2024-01-01T00:00:02.000000001Z' ORDER BY g1, g2);
RETURN (SELECT * FROM v1 WHERE g1 > '' AND g1 < 'b' ORDER BY g1, g2 LIMIT 3) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 > '' AND g1 < 'b' ORDER BY g1, g2 LIMIT 3);
RETURN (SELECT * FROM v1 WHERE g1 = 'é' AND c > 1 ORDER BY g1, g2) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 = 'é' AND c > 1 ORDER BY g1, g2);
RETURN (SELECT * FROM v1 WHERE g1 = 1 OR g2 = 'a' ORDER BY g1, g2) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 = 1 OR g2 = 'a' ORDER BY g1, g2);
RETURN (SELECT * FROM v2 WHERE a = 'a' AND b >= d'2024-01-01T00:00:01.2Z' ORDER BY a, b) == (SELECT * FROM v2 WITH NOINDEX WHERE a = 'a' AND b >= d'2024-01-01T00:00:01.2Z' ORDER BY a, b);
RETURN (SELECT * FROM v2 WHERE a IN ['', 'b'] ORDER BY b DESC, a LIMIT 4) == (SELECT * FROM v2 WITH NOINDEX WHERE a IN ['', 'b'] ORDER BY b DESC, a LIMIT 4);
RETURN (SELECT * FROM v3 WHERE g3 = 0 AND g1 = 'a' ORDER BY g3, g1) == (SELECT * FROM v3 WITH NOINDEX WHERE g3 = 0 AND g1 = 'a' ORDER BY g3, g1);
RETURN (SELECT * FROM v3 WHERE g3 >= -1.5 AND g3 < 1 ORDER BY g3, g1) == (SELECT * FROM v3 WITH NOINDEX WHERE g3 >= -1.5 AND g3 < 1 ORDER BY g3, g1);
RETURN (SELECT * FROM v5 WHERE h = 'b' ORDER BY g1) == (SELECT * FROM v5 WITH NOINDEX WHERE h = 'b' ORDER BY g1);
RETURN (SELECT * FROM v5 WHERE h > 'a' AND g1 < 'é' ORDER BY g1) == (SELECT * FROM v5 WITH NOINDEX WHERE h > 'a' AND g1 < 'é' ORDER BY g1);
RETURN (SELECT * FROM v8 WHERE sec = d'2024-01-01T00:00:01Z' ORDER BY sec, g1) == (SELECT * FROM v8 WITH NOINDEX WHERE sec = d'2024-01-01T00:00:01Z' ORDER BY sec, g1);
RETURN (SELECT * FROM v8 WHERE sec > d'2024-01-01T00:00:00.5Z' AND g1 = 'a' ORDER BY sec, g1) == (SELECT * FROM v8 WITH NOINDEX WHERE sec > d'2024-01-01T00:00:00.5Z' AND g1 = 'a' ORDER BY sec, g1);
RETURN (SELECT * FROM v10 WHERE g1 = 'a' ORDER BY g1) == (SELECT * FROM v10 WITH NOINDEX WHERE g1 = 'a' ORDER BY g1);
RETURN (SELECT * FROM v10 WHERE g2 > 10 ORDER BY g1) == (SELECT * FROM v10 WITH NOINDEX WHERE g2 > 10 ORDER BY g1);

UPDATE src SET g1 = 'b' WHERE x % 9 = 0 RETURN NONE;
UPDATE src SET g2 = g2 + 300ms WHERE x % 4 = 1 RETURN NONE;
DELETE src WHERE g1 = 'a' AND x < 40 RETURN NONE;

RETURN (SELECT * FROM v1 WHERE g1 = 'a' ORDER BY g1, g2) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 = 'a' ORDER BY g1, g2);
RETURN (SELECT * FROM v1 WHERE g1 = 'b' AND g2 >= d'2024-01-01T00:00:01.5Z' ORDER BY g2, g1) == (SELECT * FROM v1 WITH NOINDEX WHERE g1 = 'b' AND g2 >= d'2024-01-01T00:00:01.5Z' ORDER BY g2, g1);
RETURN (SELECT * FROM v2 WHERE a = 'b' AND b < d'2024-01-01T00:00:01Z' ORDER BY a, b) == (SELECT * FROM v2 WITH NOINDEX WHERE a = 'b' AND b < d'2024-01-01T00:00:01Z' ORDER BY a, b);
RETURN (SELECT * FROM v5 WHERE h IN ['a', 'b'] ORDER BY g1) == (SELECT * FROM v5 WITH NOINDEX WHERE h IN ['a', 'b'] ORDER BY g1);
RETURN (SELECT * FROM v8 WHERE sec >= d'2024-01-01T00:00:02Z' AND g1 = 'b' ORDER BY sec, g1) == (SELECT * FROM v8 WITH NOINDEX WHERE sec >= d'2024-01-01T00:00:02Z' AND g1 = 'b' ORDER BY sec, g1);
RETURN (SELECT count() FROM v1 WHERE g1 = 'a' GROUP ALL) == (SELECT count() FROM v1 WITH NOINDEX WHERE g1 = 'a' GROUP ALL);
