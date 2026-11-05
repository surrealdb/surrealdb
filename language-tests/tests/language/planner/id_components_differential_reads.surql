/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 30000

[test]
reason = """
	Differential reads over a table whose record id names its components
	(`k`, keyed by a string, an int and a datetime). Every statement returns
	true when the default plan (which may read a key range) returns what an
	oracle returns: the same query WITH NOINDEX, or the same query over `u`,
	an unkeyed table holding the same rows with the components as stored
	fields. The rows put the awkward values next to each other: strings that
	differ only after a NUL, the extreme ints, datetimes a nanosecond apart
	across a whole second and written with offsets, and NONE and NULL in a
	field that is not part of the key. The predicates mix the bounds a key
	range takes with the ones it must leave to the filter: literals of the
	wrong type, fractional and out-of-range bounds on the int, NaN and
	infinities, NONE and wrong-typed parameters, closures, correlated
	subqueries, `$this`, element paths beneath a component, and indexes into
	the id that name no component. Orders run in each direction over each
	component, with COLLATE and NUMERIC, with an alias that shadows a
	component, under LIMIT and START. Record ranges, ONLY, GROUP BY, SPLIT
	and a block that updates by the same predicate are compared too.
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
*/

DEFINE TABLE k SCHEMALESS;
DEFINE FIELD id ON k TYPE [s: string, i: int, t: datetime];
DEFINE TABLE u SCHEMALESS;
LET $strs = ['', 'a', 'a\0', 'a\0b', 'ab', 'B', 'é'];
LET $ints = [-9223372036854775808, -1, 0, 2, 7, 9223372036854775807];
LET $times = [
	d'1969-12-31T23:59:59.999999999Z',
	d'1970-01-01T00:00:00Z',
	d'2024-01-01T00:00:00Z',
	d'2024-01-01T00:00:00.000000001Z',
	d'2024-01-01T00:00:00.5+00:00',
	d'2024-01-01T01:00:00.999999999+01:00',
	d'2024-01-01T00:00:01Z',
	d'2024-01-01T00:00:01.1Z',
	d'9999-12-31T23:59:59.999999999Z'
];
FOR $a IN 0..7 {
	FOR $b IN 0..6 {
		FOR $c IN 0..9 {
			LET $n = $a * 100 + $b * 10 + $c;
			IF ($a * 7 + $b * 3 + $c) % 3 != 1 {
				LET $v = [NONE, NULL, 1, 'x', true][$n % 5];
				CREATE k SET s = $strs[$a], i = $ints[$b], t = $times[$c], v = $v, n = $n, tags = [$n % 2, $n % 3] RETURN NONE;
				CREATE u SET s = $strs[$a], i = $ints[$b], t = $times[$c], v = $v, n = $n, tags = [$n % 2, $n % 3] RETURN NONE;
			};
		};
	};
};
LET $none = NONE;
LET $wrong = 'x';
LET $f = |$x: any| $x;
RETURN count(SELECT * FROM k) == count(SELECT * FROM u) AND count(SELECT * FROM k) > 200;

-- An equality prefix and a range, against the unkeyed twin.
RETURN (SELECT s, i, t, n FROM k WHERE s = 'a' AND i = 2 AND t > d'2024-01-01T00:00:00Z' ORDER BY t, n) == (SELECT s, i, t, n FROM u WHERE s = 'a' AND i = 2 AND t > d'2024-01-01T00:00:00Z' ORDER BY t, n);
RETURN (SELECT s, i, t, n FROM k WHERE s = 'a' AND i = 2 AND t >= d'2024-01-01T01:00:00.5+01:00' AND t < d'2024-01-01T00:00:01.000000001Z' ORDER BY n) == (SELECT s, i, t, n FROM u WHERE s = 'a' AND i = 2 AND t >= d'2024-01-01T01:00:00.5+01:00' AND t < d'2024-01-01T00:00:01.000000001Z' ORDER BY n);
RETURN (SELECT s, i, t, n FROM k WHERE s = 'ab' AND i IN [0, 7, 2.0, 7dec] AND t <= d'2024-01-01T00:00:00.999999999Z' ORDER BY i DESC, t DESC) == (SELECT s, i, t, n FROM u WHERE s = 'ab' AND i IN [0, 7, 2.0, 7dec] AND t <= d'2024-01-01T00:00:00.999999999Z' ORDER BY i DESC, t DESC);
RETURN (SELECT s, i, t, n FROM k WHERE s IN ['a', 'a\0', 'a\0b'] ORDER BY s, i, t) == (SELECT s, i, t, n FROM u WHERE s IN ['a', 'a\0', 'a\0b'] ORDER BY s, i, t);
RETURN (SELECT s, i, t, n FROM k WHERE s > 'a' AND s < 'ab' ORDER BY s DESC, i, t) == (SELECT s, i, t, n FROM u WHERE s > 'a' AND s < 'ab' ORDER BY s DESC, i, t);
RETURN (SELECT s, i, t, n FROM k WHERE 'a\0' <= s AND s <= 'a\0b' ORDER BY n) == (SELECT s, i, t, n FROM u WHERE 'a\0' <= s AND s <= 'a\0b' ORDER BY n);

-- Int bounds: fractional, out of range, NaN and infinities, and equality with a non-integral value.
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i > 2.5 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i > 2.5 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i < -0.5 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i < -0.5 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i >= 9.3e18 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i >= 9.3e18 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i <= -9.3e18 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i <= -9.3e18 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i < 9.3e18 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i < 9.3e18 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i > NaN ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i > NaN ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i < math::inf AND i > -math::inf ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i < math::inf AND i > -math::inf ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i = 2.5 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i = 2.5 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i = 2dec ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i = 2dec ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i IN [2.5, 7] ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i IN [2.5, 7] ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'B' AND i = -9223372036854775808 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'B' AND i = -9223372036854775808 ORDER BY n);

-- Literals of the wrong type, NONE and NULL.
RETURN (SELECT VALUE n FROM k WHERE s = 1 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 1 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i = '2' ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND i = '2' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i = 2 AND t = '2024-01-01T00:00:00Z' ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND i = 2 AND t = '2024-01-01T00:00:00Z' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i = 2 AND t > 1 ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND i = 2 AND t > 1 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i > d'2024-01-01T00:00:00Z' ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND i > d'2024-01-01T00:00:00Z' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s < NONE ORDER BY n) == (SELECT VALUE n FROM u WHERE s < NONE ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s > NULL AND s < 'a\0' ORDER BY n) == (SELECT VALUE n FROM u WHERE s > NULL AND s < 'a\0' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'é' AND i = 0 AND t IN [NONE, d'2024-01-01T00:00:00Z', d'2024-01-01T00:00:01.1Z'] ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'é' AND i = 0 AND t IN [NONE, d'2024-01-01T00:00:00Z', d'2024-01-01T00:00:01.1Z'] ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND t > $none ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND t > $none ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i >= $wrong ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND i >= $wrong ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i = 2 AND t >= $f(d'2024-01-01T00:00:00.5Z') ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND i = 2 AND t >= $f(d'2024-01-01T00:00:00.5Z') ORDER BY n);

-- OR, NOT and predicates a key range never takes.
RETURN (SELECT VALUE n FROM k WHERE (s = 'a' OR s = 'B') AND i = 0 ORDER BY n) == (SELECT VALUE n FROM u WHERE (s = 'a' OR s = 'B') AND i = 0 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE !(s = 'a') AND i = 2 ORDER BY n) == (SELECT VALUE n FROM u WHERE !(s = 'a') AND i = 2 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND (i = 2 OR t < d'2000-01-01T00:00:00Z') ORDER BY n) == (SELECT VALUE n FROM u WHERE s = 'a' AND (i = 2 OR t < d'2000-01-01T00:00:00Z') ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s != 'a' AND i NOT IN [0, 2] AND v IS NONE ORDER BY n) == (SELECT VALUE n FROM u WHERE s != 'a' AND i NOT IN [0, 2] AND v IS NONE ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE $this.s = 'ab' AND i = 7 ORDER BY n) == (SELECT VALUE n FROM u WHERE $this.s = 'ab' AND i = 7 ORDER BY n);

-- Indexes into the id that name no component, and paths beneath one.
RETURN (SELECT VALUE n FROM k WHERE id[1] = 2 AND id[0] = 'a' ORDER BY n) == (SELECT VALUE n FROM k WITH NOINDEX WHERE id[1] = 2 AND id[0] = 'a' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE id[-1] = d'2024-01-01T00:00:00Z' ORDER BY n) == (SELECT VALUE n FROM k WITH NOINDEX WHERE id[-1] = d'2024-01-01T00:00:00Z' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE id[1.0] = 2 ORDER BY n) == (SELECT VALUE n FROM k WITH NOINDEX WHERE id[1.0] = 2 ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE id[3] = NONE AND s = 'a' ORDER BY n) == (SELECT VALUE n FROM k WITH NOINDEX WHERE id[3] = NONE AND s = 'a' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s[0] = 'a' ORDER BY n) == (SELECT VALUE n FROM u WHERE s[0] = 'a' ORDER BY n);
RETURN (SELECT VALUE n FROM k WHERE s.x = NONE AND s = 'B' ORDER BY n) == (SELECT VALUE n FROM u WHERE s.x = NONE AND s = 'B' ORDER BY n);

-- Orders in each direction, under LIMIT and START.
RETURN (SELECT VALUE n FROM k WHERE s = 'a' ORDER BY i DESC, t DESC LIMIT 3 START 1) == (SELECT VALUE n FROM u WHERE s = 'a' ORDER BY i DESC, t DESC LIMIT 3 START 1);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i = 2 ORDER BY t LIMIT 2) == (SELECT VALUE n FROM u WHERE s = 'a' AND i = 2 ORDER BY t LIMIT 2);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' AND i = 2 ORDER BY t DESC LIMIT 2 START 1) == (SELECT VALUE n FROM u WHERE s = 'a' AND i = 2 ORDER BY t DESC LIMIT 2 START 1);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' ORDER BY t DESC LIMIT 0) == (SELECT VALUE n FROM u WHERE s = 'a' ORDER BY t DESC LIMIT 0);
RETURN (SELECT VALUE n FROM k WHERE s = 'a' ORDER BY i, t START 1000) == (SELECT VALUE n FROM u WHERE s = 'a' ORDER BY i, t START 1000);
RETURN (SELECT VALUE n FROM k WHERE i = 0 ORDER BY id DESC LIMIT 4) == (SELECT VALUE n FROM u WHERE i = 0 ORDER BY s DESC, i DESC, t DESC LIMIT 4);
RETURN (SELECT VALUE n FROM k WHERE s = 'é' ORDER BY id LIMIT 5) == (SELECT VALUE n FROM u WHERE s = 'é' ORDER BY s, i, t LIMIT 5);
RETURN (SELECT VALUE [s, i, t] FROM k WHERE s IN ['B', 'a', 'é'] ORDER BY s COLLATE, i, t) == (SELECT VALUE [s, i, t] FROM u WHERE s IN ['B', 'a', 'é'] ORDER BY s COLLATE, i, t);
RETURN (SELECT VALUE [s, i, t] FROM k WHERE s = 'a' ORDER BY i NUMERIC DESC, t LIMIT 4) == (SELECT VALUE [s, i, t] FROM u WHERE s = 'a' ORDER BY i NUMERIC DESC, t LIMIT 4);
RETURN (SELECT n AS i, s FROM k WHERE s = 'a' ORDER BY i DESC LIMIT 3) == (SELECT n AS i, s FROM k WITH NOINDEX WHERE s = 'a' ORDER BY i DESC LIMIT 3);
RETURN (SELECT i AS t, t AS i, n FROM k WHERE s = 'ab' ORDER BY t, i LIMIT 4) == (SELECT i AS t, t AS i, n FROM u WHERE s = 'ab' ORDER BY t, i LIMIT 4);
RETURN (SELECT i AS q, n FROM k WHERE s = 'ab' ORDER BY q DESC, n LIMIT 3) == (SELECT i AS q, n FROM u WHERE s = 'ab' ORDER BY q DESC, n LIMIT 3);

-- Grouping, SPLIT, ONLY and a correlated bound evaluated for each outer row.
RETURN (SELECT s, count() AS c FROM k WHERE i >= 0 GROUP BY s ORDER BY s) == (SELECT s, count() AS c FROM u WHERE i >= 0 GROUP BY s ORDER BY s);
RETURN (SELECT count() FROM k WHERE s = 'a\0' AND t < d'2024-01-01T00:00:01Z' GROUP ALL) == (SELECT count() FROM u WHERE s = 'a\0' AND t < d'2024-01-01T00:00:01Z' GROUP ALL);
RETURN array::sort((SELECT n, tags FROM k WHERE s = 'a' AND i = 7 SPLIT tags)) == array::sort((SELECT n, tags FROM u WHERE s = 'a' AND i = 7 SPLIT tags));
RETURN (SELECT * FROM ONLY k WHERE s = 'a' AND i = 2 ORDER BY t DESC LIMIT 1) == (SELECT * FROM ONLY k WITH NOINDEX WHERE s = 'a' AND i = 2 ORDER BY t DESC LIMIT 1);
RETURN (SELECT n, (SELECT VALUE n FROM k WHERE s = $parent.s AND i = $parent.i AND t > $parent.t ORDER BY t LIMIT 1) AS nxt FROM k WHERE s = 'a' ORDER BY n) == (SELECT n, (SELECT VALUE n FROM u WHERE s = $parent.s AND i = $parent.i AND t > $parent.t ORDER BY t LIMIT 1) AS nxt FROM u WHERE s = 'a' ORDER BY n);

-- Record ranges and lookups on the keyed table.
RETURN array::sort((SELECT * FROM k:['a', 0, NONE]..['a', 7, NONE])) == array::sort((SELECT * FROM k WITH NOINDEX WHERE id >= k:['a', 0, NONE] AND id < k:['a', 7, NONE]));
RETURN array::sort((SELECT * FROM k:['a', 2, d'2024-01-01T00:00:00.000000001Z']..=['a', 2, d'2024-01-01T00:00:01Z'])) == array::sort((SELECT * FROM k WITH NOINDEX WHERE id >= k:['a', 2, d'2024-01-01T00:00:00.000000001Z'] AND id <= k:['a', 2, d'2024-01-01T00:00:01Z']));
RETURN (SELECT * FROM k:['ab', 7, d'2024-01-01T00:00:00Z']) == (SELECT * FROM k WITH NOINDEX WHERE id = k:['ab', 7, d'2024-01-01T00:00:00Z']);

-- An update selects the rows the oracle selects.
RETURN {
	LET $a = array::sort((SELECT VALUE id FROM k WITH NOINDEX WHERE s = 'a' AND i = 2 AND t >= d'2024-01-01T00:00:00.5Z'));
	LET $b = array::sort((UPDATE k SET hit = true WHERE s = 'a' AND i = 2 AND t >= d'2024-01-01T00:00:00.5Z' RETURN VALUE id));
	LET $c = array::sort((SELECT VALUE id FROM k WITH NOINDEX WHERE hit = true));
	UPDATE k UNSET hit RETURN NONE;
	$a == $b AND $a == $c AND array::len($a) > 0
};
