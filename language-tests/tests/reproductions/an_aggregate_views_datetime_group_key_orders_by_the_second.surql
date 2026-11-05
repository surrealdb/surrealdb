/**
[env]
namespace = true
database = true
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	An aggregated view stores each group under a record id built from its
	group columns, encoded exactly as a named `id` field's components are, so
	a datetime group key orders its keys by the whole second alone. Two groups
	falling in the same second therefore come back from a scan in neither
	group order nor `id` order, and `ORDER BY id` over such a view keeps its
	`Sort` — at a stamp as much as now, since what structures the keys is the
	view rather than anything a record id declares.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[by_time:[d'2024-01-01T00:00:00Z'], by_time:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        TableScan [ctx: Db] [table: by_time, direction: Forward]
''''

[[test.results]]
value = "[by_time:[d'2024-01-01T00:00:00Z'], by_time:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = "[by_time:[d'2024-01-01T00:00:00Z'], by_time:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = '''
"VersionScope [ctx: Db] [version: d'2030-01-01T00:00:00Z']
    ProjectValue [ctx: Db] [expr: id]
        Sort [ctx: Db] [sort_keys: id ASC]
            TableScan [ctx: Db] [table: by_time, direction: Forward]
"'''
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE by_time AS SELECT at, count() AS n FROM src GROUP BY at;
-- Written out of order, and inside one second, so key order is neither.
CREATE src SET at = d'2024-01-01T00:00:00.500Z' RETURN NONE;
CREATE src SET at = d'2024-01-01T00:00:00Z' RETURN NONE;
SLEEP 50ms;
LET $ts = time::now();

SELECT VALUE id FROM by_time ORDER BY id;
EXPLAIN SELECT VALUE id FROM by_time ORDER BY id;
SELECT VALUE id FROM by_time ORDER BY id VERSION $ts;
SELECT VALUE id FROM by_time ORDER BY id VERSION d'2030-01-01T00:00:00Z';
EXPLAIN SELECT VALUE id FROM by_time ORDER BY id VERSION d'2030-01-01T00:00:00Z';
