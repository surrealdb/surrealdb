/**
[env]
namespace = true
database = true
backend = ["surrealkv"]
versioned = true
timeout = 15000

[test]
reason = """
	Setup for `a_versioned_scan_keeps_its_sort_when_the_id_names_are_gone`:
	writes records under a record id naming a datetime component, so their
	keys are encoded by the whole second, then removes the names while those
	records stay. Runs as a request of its own so the removal is committed
	before the reads that import this.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: r:[d'2024-01-01T00:00:00Z'], n: 1 }]"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00.500Z', id: r:[d'2024-01-01T00:00:00.500Z'], n: 2 }]"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE r SCHEMALESS;
DEFINE FIELD id ON r TYPE [at: datetime];
CREATE r SET at = d'2024-01-01T00:00:00Z', n = 1;
CREATE r SET at = d'2024-01-01T00:00:00.500Z', n = 2;
REMOVE FIELD id ON r;
