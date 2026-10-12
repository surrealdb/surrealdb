/**
[env]
namespace = true
database = true
backend = ["surrealkv"]
versioned = true
timeout = 15000
imports = ["reproductions/a_versioned_scan_keeps_its_sort_when_the_id_names_are_gone_import.surql"]

[test]
reason = """
	A record id's names can be removed while the records written under them
	stay, so their keys keep the whole-second encoding of the datetime
	component they were written with. A read at a stamp lying after that
	removal must still sort: the keys it walks are the coarse ones, whatever
	the names say by then. The stamp here is a literal, so the planner reads
	the structure at it rather than declining to judge, and the removal was
	committed by an earlier request.
"""

[[test.results]]
value = "[r:[d'2024-01-01T00:00:00Z'], r:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = '''
"VersionScope [ctx: Db] [version: d'2030-01-01T00:00:00Z']
    ProjectValue [ctx: Db] [expr: id]
        Sort [ctx: Db] [sort_keys: id ASC]
            TableScan [ctx: Db] [table: r, direction: Forward]
"'''
*/

-- A stamp after the removal, and a stamp the removal is not yet visible at:
-- the scan is sorted either way, and the rows come back in `id` order.
SELECT VALUE id FROM r ORDER BY id VERSION d'2030-01-01T00:00:00Z';
EXPLAIN SELECT VALUE id FROM r ORDER BY id VERSION d'2030-01-01T00:00:00Z';
