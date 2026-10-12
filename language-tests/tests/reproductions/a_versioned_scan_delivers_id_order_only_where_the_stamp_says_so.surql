/**
[env]
namespace = true
database = true
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	A scan at a stamp delivers `ORDER BY id` exactly where the record-id
	structure of the stamp orders the keys by `id`. A table whose record id
	names no component delivers it, so the `Sort` goes, descending as well as
	ascending — the scan reads backward for the reverse. A table whose record
	id names a datetime component orders its keys by the whole second alone,
	so its `Sort` stays, and a stamp only the run can evaluate names no state
	to read a structure at, so the `Sort` stays there whatever the table.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, n: 1 }]"

[[test.results]]
value = "[{ id: item:2, n: 2 }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: reading:[d'2024-01-01T00:00:00Z'], n: 1 }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00.500Z', id: reading:[d'2024-01-01T00:00:00.500Z'], n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $ts]
    ProjectValue [ctx: Db] [expr: id]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $ts]
    ProjectValue [ctx: Db] [expr: id]
        TableScan [ctx: Db] [table: item, direction: Backward]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $ts + 0ns]
    ProjectValue [ctx: Db] [expr: id]
        Sort [ctx: Db] [sort_keys: id ASC]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $ts]
    ProjectValue [ctx: Db] [expr: id]
        Sort [ctx: Db] [sort_keys: id ASC]
            TableScan [ctx: Db] [table: reading, direction: Forward]
''''

[[test.results]]
value = "[item:1, item:2]"

[[test.results]]
value = "[item:2, item:1]"

[[test.results]]
value = "[reading:[d'2024-01-01T00:00:00Z'], reading:[d'2024-01-01T00:00:00.500Z']]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE TABLE reading SCHEMALESS;
DEFINE FIELD id ON reading TYPE [at: datetime];
CREATE item:1 SET n = 1;
CREATE item:2 SET n = 2;
CREATE reading SET at = d'2024-01-01T00:00:00Z', n = 1;
CREATE reading SET at = d'2024-01-01T00:00:00.500Z', n = 2;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- A plain record id walks its key space in `id` order at the stamp.
EXPLAIN SELECT VALUE id FROM item ORDER BY id VERSION $ts;
-- The reverse of that order is the same key space read backward.
EXPLAIN SELECT VALUE id FROM item ORDER BY id DESC VERSION $ts;
-- A stamp only the run can evaluate names no structure to judge.
EXPLAIN SELECT VALUE id FROM item ORDER BY id VERSION $ts + 0ns;
-- A datetime component orders its keys by the whole second alone.
EXPLAIN SELECT VALUE id FROM reading ORDER BY id VERSION $ts;
-- The rows themselves come back in `id` order whichever plan answers.
SELECT VALUE id FROM item ORDER BY id VERSION $ts;
SELECT VALUE id FROM item ORDER BY id DESC VERSION $ts;
SELECT VALUE id FROM reading ORDER BY id VERSION $ts;
