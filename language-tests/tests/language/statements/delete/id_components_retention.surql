/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Retention over a table whose record ids name their components, with
	indexes on those components: the ids of the rows older than a bound are
	read through the index into a parameter, and deleting them removes the rows
	and every index entry they held, so nothing leaks into a later read, count
	or UNIQUE check. The bound lies inside a second shared with a row that must
	survive (a datetime index orders by the whole second, so its scan lists the
	row and the predicate refuses it), and a DELETE with the WHERE itself
	removes the same rows of a range that ends inside a second. `ret` is keyed
	by a feed, a datetime and an int: eight rows, `n` 0 to 7, one every 400ms
	from 2024-01-01T00:00:00Z, the feed `f` for an even `n` and `g` for an odd
	one.
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
value = "[{ count: 8 }]"

# The ids older than 00:00:01.600, which is a row's own instant: four rows.
[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
match = '''string::contains($result, "IndexScan") AND string::contains($result, "index: by_at") AND string::contains($result, "access: <=d'2024-01-01T00:00:01Z'") AND string::contains($result, "Filter [") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[]"

# The row at the bound survived, and no entry is left for a deleted row.
[[test.results]]
value = "[4, 5, 6, 7]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 0 }]"

# A deleted id and a deleted UNIQUE value are free again.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 99, 4, 5, 6, 7]"

# An update of a field the index does not hold leaves its entries.
[[test.results]]
value = "[999]"

[[test.results]]
value = "[4]"

# A DELETE with the WHERE: the range ends inside the second of the row at
# 00:00:02.800, which survives.
[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 99, 4, 7]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 99, 4, 5, 6, 7]"

# The UNIQUE index still refuses a second row at a live value.
[[test.results]]
match = '''$error = /^Database index `by_at_uniq` already contains d'2024-01-01T00:00:02.800Z', with record `ret:/'''
error = true

[[test.results]]
value = "[{ count: 6 }]"

[[test.results]]
value = "true"
*/

DEFINE TABLE ret SCHEMALESS;
DEFINE FIELD id ON ret TYPE [feed: string, at: datetime, n: int];
DEFINE INDEX by_at ON ret FIELDS at;
DEFINE INDEX by_n ON ret FIELDS n;
DEFINE INDEX by_at_uniq ON ret FIELDS at UNIQUE;
FOR $n IN 0..8 {
	CREATE ret SET
		feed = ['f', 'g'][$n % 2],
		at = time::from_millis(1704067200000 + $n * 400),
		n = $n,
		v = $n * 10
		RETURN NONE;
};
SELECT count() FROM ret GROUP ALL;

LET $stale = SELECT VALUE id FROM ret WHERE at < d'2024-01-01T00:00:01.600Z';
RETURN array::len($stale);
EXPLAIN SELECT VALUE id FROM ret WHERE at < d'2024-01-01T00:00:01.600Z';
DELETE $stale;

SELECT VALUE n FROM ret ORDER BY n;
SELECT VALUE n FROM ret WHERE at < d'2024-01-01T00:00:01.600Z' ORDER BY n;
SELECT count() FROM ret WHERE n >= 0 GROUP ALL;
SELECT count() FROM ret WHERE at = d'2024-01-01T00:00:00.400Z' GROUP ALL;

CREATE ret SET feed = 'f', at = d'2024-01-01T00:00:00Z', n = 0, v = 0 RETURN NONE;
CREATE ret SET feed = 'z', at = d'2024-01-01T00:00:00.400Z', n = 99, v = 990 RETURN NONE;
SELECT VALUE n FROM ret ORDER BY at;

UPDATE ret SET v = 999 WHERE n = 4 RETURN VALUE v;
SELECT VALUE n FROM ret WHERE at = d'2024-01-01T00:00:01.600Z';

DELETE ret WHERE at >= d'2024-01-01T00:00:02Z' AND at < d'2024-01-01T00:00:02.600Z';
SELECT VALUE n FROM ret ORDER BY at;
SELECT count() FROM ret WHERE n >= 0 GROUP ALL;
SELECT count() FROM ret WHERE at = d'2024-01-01T00:00:02.400Z' GROUP ALL;

CREATE ret SET feed = 'g', at = d'2024-01-01T00:00:02Z', n = 5, v = 50 RETURN NONE;
CREATE ret SET feed = 'f', at = d'2024-01-01T00:00:02.400Z', n = 6, v = 60 RETURN NONE;
SELECT VALUE n FROM ret ORDER BY at;

CREATE ret SET feed = 'q', at = d'2024-01-01T00:00:02.800Z', n = 77, v = 770 RETURN NONE;
SELECT count() FROM ret WHERE n >= 0 GROUP ALL;
RETURN (SELECT VALUE n FROM ret WHERE at > d'2024-01-01T00:00:00.400Z' ORDER BY at) == (SELECT VALUE n FROM ret WITH NOINDEX WHERE at > d'2024-01-01T00:00:00.400Z' ORDER BY at) AND (SELECT count() FROM ret WHERE n >= 0 GROUP ALL)[0].count == (SELECT VALUE id FROM ret WITH NOINDEX).len();
