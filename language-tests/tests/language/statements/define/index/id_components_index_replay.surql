/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_index_import.surql"]
# Extra timeout because this test sleeps while the builds run.
timeout = 20000
context-timeout = 20000

[test]
reason = """
	An index defined CONCURRENTLY over the rows a table holds is built in the
	background from their stored bodies, which hold no component of the record
	id, and indexes a component as the write path does: a row the build scans
	and a row written once the build is ready land under the same entries. A
	B-tree index over a datetime component and a COUNT index guarded by a
	predicate over a component are defined over the imported `tick`; once the
	builds are ready two rows are created, one is updated and one is deleted,
	and every count, taken through the index, is that of the rows the table
	ends with, and the ranges equal a scan. A write made while a build is still
	running reaches the index through the build's queue, and on the in-memory
	store such a write may be refused with a retryable conflict, so that route
	is covered by the build tests in `kvs/index/tests.rs`, which retry it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# Each build scanned the eleven rows the table held and nothing else.
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
match = '''$result.building.status = 'ready' AND $result.building.pending = 0 AND $result.building.initial = 11'''
error = false

[[test.results]]
match = '''$result.building.status = 'ready' AND $result.building.pending = 0 AND $result.building.initial = 11'''
error = false

# Written once both indexes are ready.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# The table ends with twelve rows: the deleted one is not indexed, the two
# written are.
[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
match = '''string::contains($result, "IndexCountScan")'''
error = false

[[test.results]]
value = "[1, 5, 6, 9, 4]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/

DEFINE INDEX by_at ON tick FIELDS at CONCURRENTLY;
DEFINE INDEX tick_f ON tick COUNT WHERE feed = 'f' CONCURRENTLY;

IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };
IF (INFO FOR INDEX by_at ON tick).building.status != 'ready' OR (INFO FOR INDEX tick_f ON tick).building.status != 'ready' { SLEEP 1s };

INFO FOR INDEX by_at ON tick;
INFO FOR INDEX tick_f ON tick;

CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:04Z', n = 11 RETURN NONE;
CREATE tick SET feed = 'g', at = d'2024-01-01T00:00:04.500Z', n = 12 RETURN NONE;
DELETE tick WHERE n = 3;
UPDATE tick SET v = 1 WHERE n = 6 RETURN NONE;

SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:04Z' GROUP ALL;
SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.050Z' GROUP ALL;
SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL;
EXPLAIN SELECT count() FROM tick WHERE at = d'2024-01-01T00:00:00.500Z' GROUP ALL;
SELECT count() FROM tick WHERE feed = 'f' GROUP ALL;
EXPLAIN SELECT count() FROM tick WHERE feed = 'f' GROUP ALL;

SELECT VALUE n FROM tick WHERE at > d'2024-01-01T00:00:00.500Z' AND at <= d'2024-01-01T00:00:01.250Z' ORDER BY at, n;
FOR $b IN [
	d'2024-01-01T00:00:00Z',
	d'2024-01-01T00:00:00.500Z',
	d'2024-01-01T00:00:01Z',
	d'2024-01-01T00:00:04Z',
	d'2024-01-01T00:00:04.500Z',
] {
	IF (SELECT VALUE n FROM tick WHERE at > $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at > $b ORDER BY at, n) { THROW string::concat('rows differ for at > ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at <= $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at <= $b ORDER BY at, n) { THROW string::concat('rows differ for at <= ', <string>$b); };
	IF (SELECT VALUE n FROM tick WHERE at = $b ORDER BY at, n) != (SELECT VALUE n FROM tick WITH NOINDEX WHERE at = $b ORDER BY at, n) { THROW string::concat('rows differ for at = ', <string>$b); };
};
RETURN (SELECT count() FROM tick WHERE feed = 'f' GROUP ALL)[0].count == (SELECT VALUE id FROM tick WITH NOINDEX WHERE feed = 'f').len();
