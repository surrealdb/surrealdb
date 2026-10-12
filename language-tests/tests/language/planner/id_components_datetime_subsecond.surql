/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A datetime component's keys order by the whole second only: within one
	second they do not sort by time. A key-range scan over such a component
	reads the whole second of each bound and the predicate above it decides
	the rows within; an ordering by the component keeps its sort, which a
	LIMIT bounds and whose threshold stops the scan once it reads a later
	second; and an ordering by `id` keeps its sort over a table scan too. Six
	rows, five of them in one second, check each against the true order.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 5, 4]"

[[test.results]]
value = "[2, 1, 5, 4]"

[[test.results]]
value = "[0, 3]"

[[test.results]]
value = "[0, 3, 2]"

[[test.results]]
value = "[2, 1]"

[[test.results]]
value = "[1]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "ordered: second") AND string::contains($result, "topk_cutoff: second") AND string::contains($result, "Sort [")'''
error = false

[[test.results]]
value = "[0, 3, 2]"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[2, 1, 5]"

# `ORDER BY id` is not the order of the keys: the sort stays.
[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "Sort [")'''
error = false

[[test.results]]
value = "[0, 3, 2, 1, 5, 4]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[5, 1]"
*/

DEFINE TABLE tick SCHEMALESS;
DEFINE FIELD id ON tick TYPE [feed: string, at: datetime];
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.900Z', n = 1 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00Z', n = 0 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.500Z', n = 2 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.050Z', n = 3 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:01.250Z', n = 4 RETURN NONE;
CREATE tick SET feed = 'f', at = d'2024-01-01T00:00:00.999999999Z', n = 5 RETURN NONE;

SELECT VALUE n FROM tick WHERE feed = 'f' AND at > d'2024-01-01T00:00:00.500Z' ORDER BY at;
SELECT VALUE n FROM tick WHERE feed = 'f' AND at >= d'2024-01-01T00:00:00.500Z' ORDER BY at;
SELECT VALUE n FROM tick WHERE feed = 'f' AND at < d'2024-01-01T00:00:00.500Z' ORDER BY at;
SELECT VALUE n FROM tick WHERE feed = 'f' AND at <= d'2024-01-01T00:00:00.500Z' ORDER BY at;
SELECT VALUE n FROM tick WHERE feed = 'f' AND at > d'2024-01-01T00:00:00.050Z' AND at < d'2024-01-01T00:00:00.999999999Z' ORDER BY at;
SELECT VALUE n FROM tick WHERE feed = 'f' AND at = d'2024-01-01T00:00:00.900Z';

EXPLAIN SELECT n FROM tick WHERE feed = 'f' ORDER BY at LIMIT 3;
SELECT VALUE n FROM tick WHERE feed = 'f' ORDER BY at LIMIT 3;
SELECT VALUE n FROM tick WHERE feed = 'f' ORDER BY at DESC LIMIT 2;
SELECT VALUE n FROM tick WHERE feed = 'f' ORDER BY at LIMIT 3 START 2;

EXPLAIN SELECT n FROM tick ORDER BY id;
SELECT VALUE n FROM tick ORDER BY id;
SELECT VALUE n FROM tick ORDER BY id DESC LIMIT 1;
SELECT VALUE n FROM tick WHERE feed = 'f' ORDER BY id[1] DESC LIMIT 2 START 1;
