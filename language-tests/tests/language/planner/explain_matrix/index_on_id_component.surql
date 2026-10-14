/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_index_import.surql"]

[test]
reason = """
	A b-tree index over a component of the record id (`tick` is keyed by a
	feed and a datetime, `evt` by a source and an int) is chosen for a
	predicate on that component as for a stored field, and every case returns
	what a scan of the table returns.

	The index entry of a datetime in an index defined on this release orders
	chronologically, so a range over a datetime component seeks the bound as
	written, and the plan keeps its Filter: the predicate is judged again over
	the records the scan lists. The rows of the imported table cluster around
	whole seconds, so each of the bounds sits next to rows that a bound too
	narrow or too wide would lose. An equality is exact. An ordering by a
	datetime component is delivered by the index, which holds no Sort. A count
	over a range is read from the index entries, as are an equality's and an
	int component's. The record id an index entry ends with is keyed as the record
	is, so where the id holds a datetime no index scan, and no union of index
	scans, delivers `ORDER BY id`, and a union over a composite index delivers
	no order by a datetime column after its prefix. Bounds that cross inside
	one second admit no row.
"""

# The indexes, defined over the rows of the imported setup.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A lower bound at a whole second is that bound, exclusive.
[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_at") AND string::contains($result, "access: >d'2024-01-01T00:00:01Z'") AND string::contains($result, "Filter [") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[4, 7, 10]"

[[test.results]]
value = "[6, 9, 4, 7, 10]"

# An upper bound at a whole second is that bound; the Filter stays.
[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_at") AND string::contains($result, "access: <=d'2024-01-01T00:00:01Z'") AND string::contains($result, "Filter [") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[0, 3, 2, 8, 1, 5, 6, 9]"

[[test.results]]
value = "[0, 3, 2, 8, 1, 5]"

# Both sides, each inside a second, each exact.
[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_at") AND string::contains($result, "access: >d'2024-01-01T00:00:00.500Z' <=d'2024-01-01T00:00:01.250Z'") AND string::contains($result, "Filter [") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[1, 5, 6, 9, 4]"

# An equality is exact: two rows share the instant, one the whole second.
[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_at") AND string::contains($result, "access: = d'2024-01-01T00:00:00.500Z'") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[2, 8]"

[[test.results]]
value = "[0]"

# The component spelled as the element of the id it is.
[[test.results]]
value = "[4, 7, 10]"

# Every operator at bounds on, around and between the rows, against a scan.
[[test.results]]
value = "NONE"

# The compound index, its leading component pinned and the datetime ranged.
[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_feed_at") AND string::contains($result, "access: ['f'] MoreThan d'2024-01-01T00:00:01Z'") AND string::contains($result, "Filter [") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[4, 7]"

[[test.results]]
value = "[6, 4, 7]"

[[test.results]]
value = "[0, 3, 2, 1, 5]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_feed_at") AND string::contains($result, "access: ['f'] LessThanEqual d'2024-01-01T00:00:01Z'") AND string::contains($result, "Filter [") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[0, 3, 2, 1, 5, 6]"

[[test.results]]
value = "NONE"

# An ordering by the datetime is delivered by the index, which a LIMIT bounds.
[[test.results]]
value = "[0, 3]"

[[test.results]]
value = "[10, 7, 4]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_at") AND !string::contains($result, "Sort")'''
error = false

[[test.results]]
value = "[6, 9, 4]"

# Counts: a range, an equality and an int range are read from the index.
[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "true"

# An int component is an ordinary exact column.
[[test.results]]
value = "[40, 50]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_n") AND string::contains($result, "access: >3") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[20]"

[[test.results]]
value = "NONE"

# The record ids an index lists for one value are in no `id` order within a
# second, so `ORDER BY id` keeps its Sort, over one scan and over a union.
[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, 3, 2, 1, 5, 6, 4, 7]"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_feed") AND string::contains($result, "Sort") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[0, 3, 2]"

[[test.results]]
value = "[0, 3, 2]"

# A union over the composite index is in no `at` order within a second.
[[test.results]]
value = "[0, 3]"

# Bounds that cross inside one second.
[[test.results]]
value = "[]"
*/

DEFINE INDEX by_at ON tick FIELDS at;
DEFINE INDEX by_feed_at ON tick FIELDS feed, at;
DEFINE INDEX by_n ON evt FIELDS n;

EXPLAIN SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:01Z';
SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:01Z' ORDER BY at, n;
SELECT VALUE n FROM tick WHERE at >= d'2024-01-01T00:00:01Z' ORDER BY at, n;

EXPLAIN SELECT VALUE n FROM tick WHERE at <= d'2024-01-01T00:00:01Z';
SELECT VALUE n FROM tick WHERE at <= d'2024-01-01T00:00:01Z' ORDER BY at, n;
SELECT VALUE n FROM tick WHERE at < d'2024-01-01T00:00:01Z' ORDER BY at, n;

EXPLAIN SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:00.500Z' AND at <= d'2024-01-01T00:00:01.250Z';
SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:00.500Z' AND at <= d'2024-01-01T00:00:01.250Z' ORDER BY at, n;

EXPLAIN SELECT VALUE n FROM tick WHERE at = d'2024-01-01T00:00:00.500Z';
SELECT VALUE n FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' ORDER BY n;
SELECT VALUE n FROM tick WHERE at = d'2024-01-01T00:00:00Z' ORDER BY n;

SELECT VALUE n FROM tick WHERE id[1] > d'2024-01-01T00:00:01Z' ORDER BY at, n;

FOR $b IN [
	d'1970-01-01T00:00:00Z',
	d'2024-01-01T00:00:00Z',
	d'2024-01-01T00:00:00.050Z',
	d'2024-01-01T00:00:00.499999999Z',
	d'2024-01-01T00:00:00.500Z',
	d'2024-01-01T00:00:00.900Z',
	d'2024-01-01T00:00:00.999999999Z',
	d'2024-01-01T00:00:01Z',
	d'2024-01-01T00:00:01.250Z',
	d'2024-01-01T00:00:02Z',
	d'2024-01-01T00:00:03Z',
	d'2200-01-01T00:00:00Z',
] {
	IF (SELECT VALUE n FROM tick WHERE at > $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at > $b ORDER BY at, n) { THROW string::concat('rows differ for at > ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at >= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at >= $b ORDER BY at, n) { THROW string::concat('rows differ for at >= ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at < $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at < $b ORDER BY at, n) { THROW string::concat('rows differ for at < ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at <= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at <= $b ORDER BY at, n) { THROW string::concat('rows differ for at <= ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at = $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at = $b ORDER BY at, n) { THROW string::concat('rows differ for at = ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at > $b AND at <= d'2024-01-01T00:00:02Z' ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at > $b AND at <= d'2024-01-01T00:00:02Z' ORDER BY at, n) { THROW string::concat('rows differ for at > ', <string>$b, ' and at <= 00:00:02'); };
};

EXPLAIN SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at > d'2024-01-01T00:00:01Z';
SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at > d'2024-01-01T00:00:01Z' ORDER BY at, n;
SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z' ORDER BY at, n;
SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at < d'2024-01-01T00:00:01Z' ORDER BY at, n;
EXPLAIN SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at <= d'2024-01-01T00:00:01Z';
SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at <= d'2024-01-01T00:00:01Z' ORDER BY at, n;

FOR $f IN ['f', 'g', 'h'] {
	FOR $b IN [
		d'2024-01-01T00:00:00Z',
		d'2024-01-01T00:00:00.500Z',
		d'2024-01-01T00:00:00.999999999Z',
		d'2024-01-01T00:00:01Z',
		d'2024-01-01T00:00:01.250Z',
		d'2024-01-01T00:00:03Z',
	] {
		IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = $f AND at > $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = $f AND at > $b ORDER BY at, n) { THROW string::concat('rows differ for ', $f, ' at > ', <string>$b); };
		IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = $f AND at >= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = $f AND at >= $b ORDER BY at, n) { THROW string::concat('rows differ for ', $f, ' at >= ', <string>$b); };
		IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = $f AND at < $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = $f AND at < $b ORDER BY at, n) { THROW string::concat('rows differ for ', $f, ' at < ', <string>$b); };
		IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = $f AND at <= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = $f AND at <= $b ORDER BY at, n) { THROW string::concat('rows differ for ', $f, ' at <= ', <string>$b); };
		IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = $f AND at = $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = $f AND at = $b ORDER BY at, n) { THROW string::concat('rows differ for ', $f, ' at = ', <string>$b); };
	};
};

SELECT VALUE n FROM tick ORDER BY at LIMIT 2;
SELECT VALUE n FROM tick ORDER BY at DESC LIMIT 3;
EXPLAIN SELECT n FROM tick ORDER BY at LIMIT 2;
SELECT VALUE n FROM tick WHERE at >= d'2024-01-01T00:00:01Z' ORDER BY at, n LIMIT 3;

SELECT count() FROM tick WHERE at > d'2024-01-01T00:00:01Z' GROUP ALL;
EXPLAIN SELECT count() FROM tick WHERE at > d'2024-01-01T00:00:01Z' GROUP ALL;
SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL;
EXPLAIN SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL;
SELECT count() FROM evt WHERE n >= 2 GROUP ALL;
EXPLAIN SELECT count() FROM evt WHERE n >= 2 GROUP ALL;
RETURN (SELECT count() FROM tick WHERE at > d'2024-01-01T00:00:01Z' GROUP ALL)[0].count == (SELECT VALUE id FROM tick WITH NOINDEX WHERE at > d'2024-01-01T00:00:01Z').len() AND (SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL)[0].count == (SELECT VALUE id FROM tick WITH NOINDEX WHERE at = d'2024-01-01T00:00:00.500Z').len() AND (SELECT count() FROM evt WHERE n >= 2 GROUP ALL)[0].count == (SELECT VALUE id FROM evt WITH NOINDEX WHERE n >= 2).len();

SELECT VALUE v FROM evt WHERE n > 3 ORDER BY v;
EXPLAIN SELECT VALUE v FROM evt WHERE n > 3;
SELECT VALUE v FROM evt WHERE n = 2;
FOR $b IN [-1, 0, 2, 5, 6] {
	IF (SELECT VALUE v FROM evt WHERE n > $b ORDER BY v) != (SELECT VALUE v FROM evt WITH NOINDEX WHERE n > $b ORDER BY v) { THROW string::concat('rows differ for n > ', <string>$b); };
	IF (SELECT VALUE v FROM evt WHERE n >= $b ORDER BY v) != (SELECT VALUE v FROM evt WITH NOINDEX WHERE n >= $b ORDER BY v) { THROW string::concat('rows differ for n >= ', <string>$b); };
	IF (SELECT VALUE v FROM evt WHERE n < $b ORDER BY v) != (SELECT VALUE v FROM evt WITH NOINDEX WHERE n < $b ORDER BY v) { THROW string::concat('rows differ for n < ', <string>$b); };
	IF (SELECT VALUE v FROM evt WHERE n <= $b ORDER BY v) != (SELECT VALUE v FROM evt WITH NOINDEX WHERE n <= $b ORDER BY v) { THROW string::concat('rows differ for n <= ', <string>$b); };
	IF (SELECT VALUE v FROM evt WHERE n = $b ORDER BY v) != (SELECT VALUE v FROM evt WITH NOINDEX WHERE n = $b ORDER BY v) { THROW string::concat('rows differ for n = ', <string>$b); };
};

DEFINE INDEX by_feed ON tick FIELDS feed;
SELECT VALUE n FROM tick WITH INDEX by_feed WHERE feed = 'f' ORDER BY id;
EXPLAIN SELECT VALUE n FROM tick WITH INDEX by_feed WHERE feed = 'f' ORDER BY id;
SELECT VALUE n FROM tick WITH INDEX by_feed WHERE feed = 'f' ORDER BY id LIMIT 3;
SELECT VALUE n FROM tick WITH INDEX by_feed WHERE feed IN ['f', 'g'] ORDER BY id LIMIT 3;
SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed IN ['f', 'g'] ORDER BY at LIMIT 2;
SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:00.900Z' AND at < d'2024-01-01T00:00:00.500Z';
