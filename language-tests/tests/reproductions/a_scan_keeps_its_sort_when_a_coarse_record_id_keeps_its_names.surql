/**
[env]
namespace = true
database = true

[test]
reason = """
	A table scan is taken to deliver `ORDER BY id` unless the record id names a
	component whose keys do not walk in the order of their values. Removing the
	names left the keys as they were while the catalog stopped saying how they
	order, so two records inside one second came back in key order with the
	`Sort` removed. The removal is refused while the records it keys are stored
	out of their record ids' order, so the structure that accounts for them is
	still there when the read is planned.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: r:[d'2024-01-01T00:00:00Z'], n: 1 }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00.500Z', id: r:[d'2024-01-01T00:00:00.500Z'], n: 2 }]"

[[test.results]]
error = "Cannot remove the names from the record id of `r`: it holds r:[d'2024-01-01T00:00:00.500Z'] and r:[d'2024-01-01T00:00:00Z'], whose keys are stored in the opposite order to their record ids, which only the names account for. Delete those records, or copy them into a table with the record id you want, first"

[[test.results]]
value = "[r:[d'2024-01-01T00:00:00Z'], r:[d'2024-01-01T00:00:00.500Z']]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Sort [ctx: Db] [sort_keys: id ASC]
        TableScan [ctx: Db] [table: r, direction: Forward]
''''
*/

DEFINE FIELD id ON r TYPE [at: datetime];
CREATE r SET at = d'2024-01-01T00:00:00Z', n = 1;
CREATE r SET at = d'2024-01-01T00:00:00.500Z', n = 2;
REMOVE FIELD id ON r;
SELECT VALUE id FROM r ORDER BY id;
EXPLAIN SELECT VALUE id FROM r ORDER BY id;
