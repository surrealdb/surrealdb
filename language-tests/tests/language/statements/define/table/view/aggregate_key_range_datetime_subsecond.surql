/**
[env]
namespace = true
database = true

[test]
reason = """
The key of a datetime group is the datetime's text, in which a fraction sorts
before the whole second it follows (`10:00:00.5Z` is keyed before `10:00:00Z`),
so a key range over a datetime group cannot take its bounds as they are. A bound
is widened to the whole second it lies in, below for a lower bound and above for
an upper one, and the predicate decides the rows between. Each range here holds
instants on both sides of a whole second, and each is checked against a scan of
the view. A lower bound is used only beside a datetime upper bound: a datetime
past the year 9999 is keyed by text that opens with `+`, which sorts before every
year up to 9999, so a range opening at a lower bound alone would pass over it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T09:59:59.999Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:01Z' }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: by_instant")'''
error = false

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:01Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:01Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:01Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T09:59:59.999Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T09:59:59.999Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T09:59:59.999Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00.000000001Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00.500Z' }]"

[[test.results]]
value = "[{ n: 1, recorded_at: d'2024-01-01T10:00:00Z' }, { n: 1, recorded_at: d'2024-01-01T10:00:01Z' }]"

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
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: by_instant") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: by_instant") AND !string::contains($result, "TableScan")'''
error = false

*/
DEFINE TABLE ping SCHEMALESS;
DEFINE TABLE by_instant AS SELECT recorded_at, count() AS n FROM ping GROUP BY recorded_at;

-- Five instants within a second and a half. The key of a datetime is its text, in
-- which a fraction sorts before the whole second it follows, so the rows are keyed
-- 09:59:59.999, 10:00:00.000000001, 10:00:00.500, 10:00:00, 10:00:01 and not in
-- the order of time.
INSERT INTO ping [
	{ id: 1, recorded_at: d'2024-01-01T09:59:59.999Z' },
	{ id: 2, recorded_at: d'2024-01-01T10:00:00Z' },
	{ id: 3, recorded_at: d'2024-01-01T10:00:00.000000001Z' },
	{ id: 4, recorded_at: d'2024-01-01T10:00:00.500Z' },
	{ id: 5, recorded_at: d'2024-01-01T10:00:01Z' }
] RETURN NONE;
SELECT recorded_at, n FROM by_instant ORDER BY recorded_at;

-- A range from a whole second reads the fractions that follow it, which sort
-- before it in the key.
EXPLAIN SELECT recorded_at, n FROM by_instant WHERE recorded_at >= d'2024-01-01T10:00:00Z' AND recorded_at <= d'2024-01-01T10:00:01Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at >= d'2024-01-01T10:00:00Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at > d'2024-01-01T10:00:00Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at > d'2024-01-01T09:59:59.999Z' ORDER BY recorded_at;

-- A range up to a fraction reads the whole second, and the predicate drops the
-- instants after it.
SELECT recorded_at, n FROM by_instant WHERE recorded_at <= d'2024-01-01T10:00:00.500Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at < d'2024-01-01T10:00:00.500Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at <= d'2024-01-01T10:00:00.000000001Z' ORDER BY recorded_at;

-- Both sides inside one second.
SELECT recorded_at, n FROM by_instant WHERE recorded_at >= d'2024-01-01T10:00:00.000000001Z' AND recorded_at <= d'2024-01-01T10:00:00.500Z' ORDER BY recorded_at;

-- An equality is the instant itself, not its second.
SELECT recorded_at, n FROM by_instant WHERE recorded_at = d'2024-01-01T10:00:00Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at = d'2024-01-01T10:00:00.500Z' ORDER BY recorded_at;
SELECT recorded_at, n FROM by_instant WHERE recorded_at IN [d'2024-01-01T10:00:00Z', d'2024-01-01T10:00:01Z'] ORDER BY recorded_at;

-- Each answers what a scan of the view answers.
RETURN (SELECT recorded_at, n FROM by_instant WHERE recorded_at >= d'2024-01-01T10:00:00Z' ORDER BY recorded_at)
	= (SELECT recorded_at, n FROM by_instant WITH NOINDEX WHERE recorded_at >= d'2024-01-01T10:00:00Z' ORDER BY recorded_at);
RETURN (SELECT recorded_at, n FROM by_instant WHERE recorded_at > d'2024-01-01T09:59:59.999Z' ORDER BY recorded_at)
	= (SELECT recorded_at, n FROM by_instant WITH NOINDEX WHERE recorded_at > d'2024-01-01T09:59:59.999Z' ORDER BY recorded_at);
RETURN (SELECT recorded_at, n FROM by_instant WHERE recorded_at <= d'2024-01-01T10:00:00.500Z' ORDER BY recorded_at)
	= (SELECT recorded_at, n FROM by_instant WITH NOINDEX WHERE recorded_at <= d'2024-01-01T10:00:00.500Z' ORDER BY recorded_at);
RETURN (SELECT recorded_at, n FROM by_instant WHERE recorded_at < d'2024-01-01T10:00:00.000000001Z' ORDER BY recorded_at)
	= (SELECT recorded_at, n FROM by_instant WITH NOINDEX WHERE recorded_at < d'2024-01-01T10:00:00.000000001Z' ORDER BY recorded_at);
RETURN (SELECT recorded_at, n FROM by_instant WHERE recorded_at = d'2024-01-01T10:00:00Z' ORDER BY recorded_at)
	= (SELECT recorded_at, n FROM by_instant WITH NOINDEX WHERE recorded_at = d'2024-01-01T10:00:00Z' ORDER BY recorded_at);
RETURN (SELECT recorded_at, n FROM by_instant WHERE recorded_at >= d'2024-01-01T10:00:00.000000001Z' AND recorded_at < d'2024-01-01T10:00:01Z' ORDER BY recorded_at)
	= (SELECT recorded_at, n FROM by_instant WITH NOINDEX WHERE recorded_at >= d'2024-01-01T10:00:00.000000001Z' AND recorded_at < d'2024-01-01T10:00:01Z' ORDER BY recorded_at);

-- A lower bound alone leaves the view to a scan; beside a datetime upper bound it
-- bounds the keys.
EXPLAIN SELECT recorded_at, n FROM by_instant WHERE recorded_at > d'2024-01-01T09:59:59.999Z';
EXPLAIN SELECT recorded_at, n FROM by_instant WHERE recorded_at > d'2024-01-01T09:59:59.999Z' AND recorded_at < d'2024-01-01T10:00:01Z';
