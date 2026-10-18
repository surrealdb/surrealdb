/**
[env]
namespace = true
database = true
# Extra timeout: the test runs a few thousand nested queries, which RocksDB does not finish in 30
# seconds when the suite forces the smallest morsels.
timeout = 120000

[env.capabilities]
allow-eval-query = true

[test]
reason = """
A key range over an aggregated view's group values reads a superset of the rows
its predicate admits, whatever the view's shape and whatever the group values
hold. Views over two groups, renamed and reordered groups, a nested group path,
a mixed-case alias, a view with its own condition, a view over two sources, a
group computed by `time::floor`, a view over a view and a view with a field of
its own are maintained by writes, updates and deletes of rows whose group
values are NONE, NULL, bools, numbers of every kind, strings, durations,
datetimes with fractions of a second, uuids, arrays, objects and record links.
Every single comparison (`=`, `<`, `<=`, `>`, `>=`, `IN`) of each addressable
group with literals of each of those kinds, every pinned first group with a
range on the second, and every two-sided range on the first group returns the
same rows as `WITH NOINDEX`. The counts pin how many of the single comparisons
plan a key range, so the comparison is not vacuous, and the views hold what a
re-aggregation of their sources gives.
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
value = "[]"

[[test.results]]
value = "[]"

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
value = "[{ differ: 0, ranged: 720, total: 2268 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"
*/
DEFINE TABLE src;
DEFINE TABLE src2;
LET $vals = [NONE, NULL, true, false, 1, 1.5f, 2dec, 1f, '', 'a', 'B', 'abc', 'z', 1h, 90s,
	d'2024-01-01T00:00:00Z', d'2024-01-01T00:00:00.5Z', d'2023-12-31T23:59:59.999Z', d'2024-01-01T00:00:01Z', d'2024-01-01T00:00:00.000000001Z',
	u'018f3d2e-0000-7000-8000-000000000001', u'018f3d2e-0000-7000-8000-000000000002',
	[1, 2], { x: 1 }, person:1, person:x, person:[1], 'person:1'];
LET $n = array::len($vals);
DEFINE TABLE pair AS SELECT g1, g2, count() AS n FROM src GROUP BY g1, g2;
DEFINE TABLE renamed AS SELECT g2 AS b, g1 AS a, count() AS n FROM src GROUP BY g1, g2;
DEFINE TABLE nested AS SELECT o.k, g1, count() AS n FROM src GROUP BY o.k, g1;
DEFINE TABLE cased AS SELECT g1 AS City, count() AS n FROM src GROUP BY g1;
DEFINE TABLE filtered AS SELECT g1, count() AS n FROM src WHERE v > 30 GROUP BY g1;
DEFINE TABLE sources AS SELECT g1, g2, count() AS n FROM src, src2 GROUP BY g1, g2;
DEFINE TABLE daily AS SELECT time::floor(t, 1d) AS day, g1, count() AS n FROM src GROUP BY day, g1;
DEFINE TABLE rollup AS SELECT g1, math::sum(n) AS n FROM pair GROUP BY g1;
DEFINE TABLE extra AS SELECT g1, g2, count() AS n FROM src GROUP BY g1, g2;
DEFINE FIELD note ON extra VALUE 'kept';
FOR $i IN 0..120 {
	CREATE type::record('src', $i) SET g1 = $vals[($i * 3) % $n], g2 = $vals[($i * 11 + 3) % $n], o = { k: $vals[($i * 13 + 1) % $n] }, v = $i,
		t = d'2024-01-01T00:00:00Z' + <duration> string::concat(<string> ($i * 37 % 200), 'h') + <duration> string::concat(<string> ($i * 7919 % 1000), 'ms') RETURN NONE;
	IF $i % 4 = 0 { CREATE type::record('src2', $i) SET g1 = $vals[($i * 5) % $n], g2 = $vals[($i * 3 + 1) % $n] RETURN NONE };
};
DELETE src WHERE v % 9 = 0;
UPDATE src SET g1 = $vals[(v * 17) % $n] WHERE v % 7 = 0 RETURN NONE;
LET $lits = ["''", "'a'", "'abc'", "'z'", "'person:1'", "true", "false", "90s", "1h",
	"d'2024-01-01T00:00:00Z'", "d'2024-01-01T00:00:00.5Z'", "d'2023-12-31T23:59:59.999Z'", "d'2024-01-03T00:00:00Z'",
	"u'018f3d2e-0000-7000-8000-000000000001'", "person:1", "person:x", "1", "NONE"];
LET $cols = [['pair', 'g1'], ['pair', 'id[1]'], ['renamed', 'a'], ['renamed', 'b'], ['nested', 'o.k'], ['cased', 'City'], ['cased', 'city'], ['filtered', 'g1'], ['sources', 'g1'], ['daily', 'day'], ['rollup', 'g1'], ['extra', 'g1']];
FOR $c IN $cols {
	FOR $op IN ['=', '<', '<=', '>', '>=', 'IN'] {
		FOR $l IN $lits {
			LET $rhs = IF $op = 'IN' { string::concat('[', $l, ", 'zz']") } ELSE { $l };
			LET $w = string::concat($c[1], ' ', $op, ' ', $rhs);
			LET $q = string::concat('SELECT VALUE id FROM ', $c[0], ' WHERE ', $w);
			LET $a = array::sort(eval::surql($q));
			LET $b = array::sort(eval::surql(string::concat('SELECT VALUE id FROM ', $c[0], ' WITH NOINDEX WHERE ', $w)));
			LET $e = <string> eval::surql(string::concat('EXPLAIN ', $q));
			CREATE result SET q = $q, same = $a == $b, ranged = string::contains($e, 'KeyRangeScan') RETURN NONE;
		};
	};
};
FOR $l1 IN $lits { FOR $op IN ['<', '>='] { FOR $l2 IN $lits {
	LET $w = string::concat('g1 = ', $l1, ' AND g2 ', $op, ' ', $l2);
	LET $a = array::sort(eval::surql(string::concat('SELECT VALUE id FROM pair WHERE ', $w)));
	LET $b = array::sort(eval::surql(string::concat('SELECT VALUE id FROM pair WITH NOINDEX WHERE ', $w)));
	CREATE result SET q = $w, same = $a == $b, ranged = NONE RETURN NONE;
}}};
FOR $l1 IN $lits { FOR $l2 IN $lits {
	LET $w = string::concat('g1 > ', $l1, ' AND g1 <= ', $l2);
	LET $a = array::sort(eval::surql(string::concat('SELECT VALUE id FROM pair WHERE ', $w)));
	LET $b = array::sort(eval::surql(string::concat('SELECT VALUE id FROM pair WITH NOINDEX WHERE ', $w)));
	CREATE result SET q = $w, same = $a == $b, ranged = NONE RETURN NONE;
}};
SELECT count() AS total, count(ranged = true) AS ranged, count(!same) AS differ FROM result GROUP ALL;
RETURN (SELECT g1, g2, n FROM pair ORDER BY g1, g2) == (SELECT g1, g2, count() AS n FROM src GROUP BY g1, g2 ORDER BY g1, g2);
RETURN (SELECT g1, g2, n FROM sources ORDER BY g1, g2) == (SELECT g1, g2, count() AS n FROM src, src2 GROUP BY g1, g2 ORDER BY g1, g2);
SELECT VALUE q FROM result WHERE !same;
