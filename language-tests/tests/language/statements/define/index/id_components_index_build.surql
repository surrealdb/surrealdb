/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_index_import.surql"]
# Extra timeout because the test rebuilds indexes and runs many queries.
timeout = 10000
context-timeout = 10000

[test]
reason = """
	An index defined over the rows a table already holds reads each row's
	components off its record id, which the stored body does not hold: a build
	that evaluated a column over the body alone would index every row under
	NONE. B-tree, UNIQUE and COUNT indexes (the COUNT one guarded by a
	predicate over components) are defined over the rows of the imported
	`tick` and `evt`, and the entries they hold are read back through
	index-only counts, whose numbers a build that indexed nothing could not
	give. A UNIQUE index over a component that repeats is refused at build and
	left undefined. Rebuilding each index gives the same entries, a write after
	the build is maintained, and every answer equals that of a scan.
"""

# The indexes, built over the rows of the imported setup.
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

# A UNIQUE index over a component the rows repeat is refused and not kept.
[[test.results]]
match = '''$error = /^Database index `dup_src` already contains 'a', with record `evt:\['a', [024]\]`$/'''
error = true

[[test.results]]
match = '''$result.indexes.dup_src = NONE AND $result.indexes.by_n_uniq = "DEFINE INDEX by_n_uniq ON evt FIELDS n UNIQUE"'''
error = false

# Entries are counted per instant: two rows share 00:00:00.500 and two the
# whole second 00:00:01.
[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 6 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

# Ranges over the built index return what a scan returns.
[[test.results]]
value = "[1, 5, 6, 9, 4]"

[[test.results]]
value = "NONE"

# The COUNT indexes, guarded by predicates over components.
[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "true"

# Rebuilding gives the same entries.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 2, 6, 3, 3]"

[[test.results]]
value = "[1, 5, 6, 9, 4]"

# A write after the build is maintained.
[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "true"
*/

DEFINE INDEX by_at ON tick FIELDS at;
DEFINE INDEX by_feed_at ON tick FIELDS feed, at;
DEFINE INDEX by_n_uniq ON evt FIELDS n UNIQUE;
DEFINE INDEX evt_a ON evt COUNT WHERE src = 'a';
DEFINE INDEX tick_f ON tick COUNT WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z';

DEFINE INDEX dup_src ON evt FIELDS src UNIQUE;
INFO FOR TABLE evt;

SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL;
EXPLAIN SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL;
SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:01Z' GROUP ALL;
SELECT count() FROM evt WHERE n >= 0 GROUP ALL;
EXPLAIN SELECT count() FROM evt WHERE n >= 0 GROUP ALL;

SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:00.500Z' AND at <= d'2024-01-01T00:00:01.250Z' ORDER BY at, n;
FOR $b IN [
	d'2024-01-01T00:00:00Z',
	d'2024-01-01T00:00:00.500Z',
	d'2024-01-01T00:00:00.999999999Z',
	d'2024-01-01T00:00:01Z',
	d'2024-01-01T00:00:01.250Z',
	d'2024-01-01T00:00:03Z',
] {
	IF (SELECT VALUE n FROM tick WHERE at > $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at > $b ORDER BY at, n) { THROW string::concat('rows differ for at > ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at <= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at <= $b ORDER BY at, n) { THROW string::concat('rows differ for at <= ', <string>$b); };
	IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at >= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = 'f' AND at >= $b ORDER BY at, n) { THROW string::concat('rows differ for feed f, at >= ', <string>$b); };
	IF (SELECT VALUE n FROM tick WITH INDEX by_feed_at WHERE feed = 'f' AND at < $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE feed = 'f' AND at < $b ORDER BY at, n) { THROW string::concat('rows differ for feed f, at < ', <string>$b); };
};

SELECT count() FROM evt WHERE src = 'a' GROUP ALL;
EXPLAIN SELECT count() FROM evt WHERE src = 'a' GROUP ALL;
SELECT count() FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z' GROUP ALL;
EXPLAIN SELECT count() FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z' GROUP ALL;
RETURN (SELECT count() FROM evt WHERE src = 'a' GROUP ALL)[0].count == (SELECT VALUE id FROM evt WITH NOINDEX WHERE src = 'a').len() AND (SELECT count() FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z' GROUP ALL)[0].count == (SELECT VALUE id FROM tick WITH NOINDEX WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z').len();

REBUILD INDEX by_at ON tick;
REBUILD INDEX tick_f ON tick;
REBUILD INDEX evt_a ON evt;
REBUILD INDEX by_n_uniq ON evt;
RETURN [
	(SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL)[0].count,
	(SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:01Z' GROUP ALL)[0].count,
	(SELECT count() FROM evt WHERE n >= 0 GROUP ALL)[0].count,
	(SELECT count() FROM evt WHERE src = 'a' GROUP ALL)[0].count,
	(SELECT count() FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z' GROUP ALL)[0].count,
];
SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:00.500Z' AND at <= d'2024-01-01T00:00:01.250Z' ORDER BY at, n;

CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:04Z', n = 11 RETURN NONE;
SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:04Z' GROUP ALL;
SELECT count() FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:01Z' GROUP ALL;
RETURN (SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:04Z' GROUP ALL)[0].count == (SELECT VALUE id FROM tick WITH NOINDEX WHERE at = d'2024-01-01T00:00:04Z').len();
