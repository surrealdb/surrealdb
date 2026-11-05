/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view keys each row by its group values (`hourly:['London',
d'2024-01-01T10:00:00Z']`), so a predicate on the projected group columns reads
the stretch of the view's keys it names and not the whole view. A group column
has no declared kind, so a bound is used only when it is a literal of a kind whose
key encoding orders exactly as its values do: a string, a uuid, a duration, a
bool or a datetime, a datetime being widened to the whole second its key text
sorts by. A number is never used, because equal numbers of different kinds (`1`,
`1.0`, `1dec`) do not share a key. The predicate is always judged again over the
rows a range yields, so every result is what a scan of the view gives, which
`WITH NOINDEX` shows beside it. A view with no group values and a view that
defines a field on a group column keep a scan. The rows a view holds are not in
the order of its groups' values, so a range delivers no ordering and an `ORDER BY`
is the sort's.
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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: hourly:['London', d'2024-01-01T10:00:00Z'], n: 2 }, { id: hourly:['London', d'2024-01-01T11:00:00Z'], n: 1 }, { id: hourly:['Paris', d'2024-01-01T10:00:00Z'], n: 1 }, { id: hourly:['Paris', d'2024-01-01T12:00:00Z'], n: 1 }, { id: hourly:['Rome', d'2024-01-02T09:00:00Z'], n: 1 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 2 }, { slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 2 }, { slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }]"

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 2 }]"

[[test.results]]
value = "[{ slot: d'2024-01-01T12:00:00Z', city: 'Paris', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T12:00:00Z', city: 'Paris', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 2 }, { slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }, { slot: d'2024-01-02T09:00:00Z', city: 'Rome', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'Paris', n: 1 }, { slot: d'2024-01-01T12:00:00Z', city: 'Paris', n: 1 }, { slot: d'2024-01-02T09:00:00Z', city: 'Rome', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'Paris', n: 1 }, { slot: d'2024-01-01T12:00:00Z', city: 'Paris', n: 1 }, { slot: d'2024-01-02T09:00:00Z', city: 'Rome', n: 1 }]"

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 2 }, { slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }, { slot: d'2024-01-01T10:00:00Z', city: 'Paris', n: 1 }, { slot: d'2024-01-01T12:00:00Z', city: 'Paris', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: hourly") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 2 }, { slot: d'2024-01-01T10:00:00Z', city: 'Paris', n: 1 }]"

[[test.results]]
value = "[{ slot: d'2024-01-02T09:00:00Z', city: 'Rome', n: 1 }, { slot: d'2024-01-01T12:00:00Z', city: 'Paris', n: 1 }, { slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: by_month_city") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[{ city: 'London', month: 1, n: 3 }, { city: 'Paris', month: 1, n: 2 }, { city: 'Rome', month: 1, n: 1 }]"

[[test.results]]
value = "[{ city: 'London', month: 1, n: 3 }, { city: 'Paris', month: 1, n: 2 }, { city: 'Rome', month: 1, n: 1 }]"

[[test.results]]
value = "[{ city: 'London', month: 1, n: 3 }, { city: 'Paris', month: 1, n: 2 }, { city: 'Rome', month: 1, n: 1 }]"

[[test.results]]
value = "[{ city: 'London', month: 1, n: 3 }, { city: 'Paris', month: 1, n: 2 }, { city: 'Rome', month: 1, n: 1 }]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: by_month_city") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[{ city: 'London', month: 1, n: 3 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: by_city_month") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ city: 'London', month: 1, n: 3 }]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: all_readings") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[{ id: all_readings:[], n: 6 }]"

[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: shadowed") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[{ city: 'London', n: 3 }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: merged") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[{ city: 'London', n: 4 }]"

[[test.results]]
value = "[{ city: 'Oslo', n: 1 }, { city: 'Rome', n: 1 }]"

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
value = "[{ slot: d'2024-01-01T10:00:00Z', city: 'London', n: 3 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ slot: d'2024-01-01T11:00:00Z', city: 'London', n: 1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: mixed") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "3"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ n: 1, tag: 'b' }, { n: 1, tag: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "true"

*/
-- Sources: the rows the views aggregate.
DEFINE TABLE reading SCHEMALESS;
DEFINE TABLE legacy SCHEMALESS;

-- A string group and a datetime group, the second an expression read under its
-- alias.
DEFINE TABLE hourly AS
	SELECT city, time::floor(recorded_at, 1h) AS slot, count() AS n
	FROM reading
	GROUP BY city, slot;

-- A string group then a number group, and the reverse.
DEFINE TABLE by_city_month AS
	SELECT city, time::month(recorded_at) AS month, count() AS n
	FROM reading
	GROUP BY city, month;
DEFINE TABLE by_month_city AS
	SELECT time::month(recorded_at) AS month, city, count() AS n
	FROM reading
	GROUP BY month, city;

-- A view with no group values.
DEFINE TABLE all_readings AS SELECT count() AS n FROM reading GROUP ALL;

-- A view that defines a field on its group column.
DEFINE TABLE shadowed AS SELECT city, count() AS n FROM reading GROUP BY city;
DEFINE FIELD city ON shadowed TYPE string;

-- A view over two sources.
DEFINE TABLE merged AS SELECT city, count() AS n FROM reading, legacy GROUP BY city;

-- Rows, written after the views so that each is maintained from the change.
INSERT INTO reading [
	{ id: 1, city: 'London', recorded_at: d'2024-01-01T10:15:00Z' },
	{ id: 2, city: 'London', recorded_at: d'2024-01-01T10:45:00Z' },
	{ id: 3, city: 'London', recorded_at: d'2024-01-01T11:05:00Z' },
	{ id: 4, city: 'Paris', recorded_at: d'2024-01-01T10:20:00Z' },
	{ id: 5, city: 'Paris', recorded_at: d'2024-01-01T12:30:00Z' },
	{ id: 6, city: 'Rome', recorded_at: d'2024-01-02T09:00:00Z' }
] RETURN NONE;
INSERT INTO legacy [
	{ id: 1, city: 'London' },
	{ id: 2, city: 'Oslo' }
] RETURN NONE;

-- The view's rows, keyed by their group values and read in key order.
SELECT id, n FROM hourly;

-- An equality on the first group reads one stretch of the view's keys.
EXPLAIN SELECT city, slot, n FROM hourly WHERE city = 'London';
SELECT city, slot, n FROM hourly WHERE city = 'London';
-- WITH NOINDEX reads the whole view, and answers the same.
EXPLAIN SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city = 'London';
SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city = 'London';

-- An equality on the first group, then a range on the second.
EXPLAIN SELECT city, slot, n FROM hourly WHERE city = 'London' AND slot >= d'2024-01-01T11:00:00Z';
SELECT city, slot, n FROM hourly WHERE city = 'London' AND slot >= d'2024-01-01T11:00:00Z';
SELECT city, slot, n FROM hourly WHERE city = 'London' AND slot < d'2024-01-01T11:00:00Z';
SELECT city, slot, n FROM hourly WHERE city = 'Paris' AND slot > d'2024-01-01T10:00:00Z' AND slot <= d'2024-01-01T12:00:00Z';

-- Both groups pinned.
EXPLAIN SELECT city, slot, n FROM hourly WHERE city = 'Paris' AND slot = d'2024-01-01T12:00:00Z';
SELECT city, slot, n FROM hourly WHERE city = 'Paris' AND slot = d'2024-01-01T12:00:00Z';

-- A list of values, written as an IN or as an OR of equalities.
EXPLAIN SELECT city, slot, n FROM hourly WHERE city IN ['London', 'Rome'];
SELECT city, slot, n FROM hourly WHERE city IN ['London', 'Rome'];
EXPLAIN SELECT city, slot, n FROM hourly WHERE city = 'Rome' OR city = 'Paris';
SELECT city, slot, n FROM hourly WHERE city = 'Rome' OR city = 'Paris' ORDER BY city, slot;

-- A range on the first group.
EXPLAIN SELECT city, slot, n FROM hourly WHERE city > 'London';
SELECT city, slot, n FROM hourly WHERE city > 'London';
SELECT city, slot, n FROM hourly WHERE city >= 'London' AND city < 'Rome';

-- A group that is not the first has no stretch of keys of its own: the view is
-- scanned.
EXPLAIN SELECT city, slot, n FROM hourly WHERE slot = d'2024-01-01T10:00:00Z';
SELECT city, slot, n FROM hourly WHERE slot = d'2024-01-01T10:00:00Z';

-- The view's rows do not come in the order of the groups' values, so an
-- ordering is the sort's and not the scan's.
SELECT city, slot, n FROM hourly WHERE city >= 'London' ORDER BY slot DESC, city LIMIT 3;

-- A number is never used as a bound: a float, a decimal and an int that are equal
-- do not share a key, so a bound would miss the rows written the other way. The
-- view is scanned, and every spelling of the month finds its rows.
EXPLAIN SELECT month, city, n FROM by_month_city WHERE month = 1;
SELECT month, city, n FROM by_month_city WHERE month = 1;
SELECT month, city, n FROM by_month_city WHERE month = 1.0;
SELECT month, city, n FROM by_month_city WHERE month = 1dec;
SELECT month, city, n FROM by_month_city WHERE month IN [1, 2];
-- The group after a number is not reached either.
EXPLAIN SELECT month, city, n FROM by_month_city WHERE month = 1 AND city = 'London';
SELECT month, city, n FROM by_month_city WHERE month = 1 AND city = 'London';
-- A number after a string group is left to the predicate; the string still
-- bounds the keys.
EXPLAIN SELECT city, month, n FROM by_city_month WHERE city = 'London' AND month = 1.0;
SELECT city, month, n FROM by_city_month WHERE city = 'London' AND month = 1.0;

-- A view with no group values has one row and no key to range over.
EXPLAIN SELECT id, n FROM all_readings WHERE n = 6;
SELECT id, n FROM all_readings WHERE n = 6;

-- A field defined on a group column may change what the column reads as, so
-- the view is scanned and the column is read as the field defines it.
EXPLAIN SELECT city, n FROM shadowed WHERE city = 'London';
SELECT city, n FROM shadowed WHERE city = 'London';

-- A view over several sources keeps one row per group across all of them.
EXPLAIN SELECT city, n FROM merged WHERE city = 'London';
SELECT city, n FROM merged WHERE city = 'London';
SELECT city, n FROM merged WHERE city IN ['Oslo', 'Rome'];

-- A range returns what a scan of the view returns.
RETURN (SELECT city, slot, n FROM hourly WHERE city = 'London')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city = 'London');
RETURN (SELECT city, slot, n FROM hourly WHERE city = 'London' AND slot >= d'2024-01-01T11:00:00Z')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city = 'London' AND slot >= d'2024-01-01T11:00:00Z');
RETURN (SELECT city, slot, n FROM hourly WHERE city IN ['London', 'Rome'] AND slot < d'2024-01-02T00:00:00Z')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city IN ['London', 'Rome'] AND slot < d'2024-01-02T00:00:00Z');
RETURN (SELECT city, slot, n FROM hourly WHERE city >= 'London' AND city < 'Rome')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city >= 'London' AND city < 'Rome');
RETURN (SELECT city, slot, n FROM hourly WHERE city > 'Zurich')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city > 'Zurich');

-- A source write moves the stored row, and the next read of its range sees it.
CREATE reading:7 SET city = 'London', recorded_at = d'2024-01-01T10:50:00Z' RETURN NONE;
SELECT city, slot, n FROM hourly WHERE city = 'London' AND slot = d'2024-01-01T10:00:00Z';
RETURN (SELECT city, slot, n FROM hourly WHERE city = 'London' AND slot = d'2024-01-01T10:00:00Z')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city = 'London' AND slot = d'2024-01-01T10:00:00Z');
DELETE reading:1, reading:2, reading:7 RETURN NONE;
SELECT city, slot, n FROM hourly WHERE city = 'London';
RETURN (SELECT city, slot, n FROM hourly WHERE city = 'London')
	= (SELECT city, slot, n FROM hourly WITH NOINDEX WHERE city = 'London');

-- A group value of any kind sits among the view's keys: NONE, then numbers, then
-- strings, then datetimes, the order in which the kinds compare. A range bounded by
-- a string reads every row its predicate admits, of whatever kind.
DEFINE TABLE mixed_src SCHEMALESS;
DEFINE TABLE mixed AS SELECT tag, count() AS n FROM mixed_src GROUP BY tag;
INSERT INTO mixed_src [
	{ id: 1, tag: 'b' },
	{ id: 2 },
	{ id: 3, tag: 7 },
	{ id: 4, tag: d'2024-01-01T00:00:00Z' },
	{ id: 5, tag: 'a' }
] RETURN NONE;
EXPLAIN SELECT tag, n FROM mixed WHERE tag < 'b';
RETURN array::len(SELECT tag, n FROM mixed WHERE tag < 'b');
RETURN (SELECT tag, n FROM mixed WHERE tag < 'b')
	= (SELECT tag, n FROM mixed WITH NOINDEX WHERE tag < 'b');
SELECT tag, n FROM mixed WHERE tag > 'a';
RETURN (SELECT tag, n FROM mixed WHERE tag > 'a')
	= (SELECT tag, n FROM mixed WITH NOINDEX WHERE tag > 'a');
