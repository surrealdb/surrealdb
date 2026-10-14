/**
[env]
namespace = true
database = true
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	A scan at a stamp delivers `ORDER BY id` only where the plan can read the
	record-id structure of the stamp and it orders the keys by `id`. A stamp
	only the run can evaluate names no state to read a structure at, so the
	`Sort` stays there whatever the table. A stamp a `LET` computes is one: the
	request is planned before the `LET` runs. A table whose record id names a
	datetime component orders its keys by the whole second alone, so its `Sort`
	stays at any stamp. The rows come back in `id` order whichever plan answers.
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
        Sort [ctx: Db] [sort_keys: id ASC]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'VersionScope [ctx: Db] [version: $ts]
    ProjectValue [ctx: Db] [expr: id]
        Sort [ctx: Db] [sort_keys: id DESC]
            TableScan [ctx: Db] [table: item, direction: Forward]
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

-- A stamp a `LET` computes is evaluated by the run, so the `Sort` stays.
EXPLAIN SELECT VALUE id FROM item ORDER BY id VERSION $ts;
-- So it does for the reverse order.
EXPLAIN SELECT VALUE id FROM item ORDER BY id DESC VERSION $ts;
-- A stamp only the run can evaluate names no structure to judge.
EXPLAIN SELECT VALUE id FROM item ORDER BY id VERSION $ts + 0ns;
-- A datetime component orders its keys by the whole second alone.
EXPLAIN SELECT VALUE id FROM reading ORDER BY id VERSION $ts;
-- The rows themselves come back in `id` order whichever plan answers.
SELECT VALUE id FROM item ORDER BY id VERSION $ts;
SELECT VALUE id FROM item ORDER BY id DESC VERSION $ts;
SELECT VALUE id FROM reading ORDER BY id VERSION $ts;
