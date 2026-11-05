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
	leaves `r` holding two records one second apart in their record ids and
	the other way round in their keys, under a table that names nothing.

	The first is written under a record id naming a datetime component, so its
	key is the whole-second encoding, and it stays when the names go: one
	record is in order with itself, which is what the names are held to on
	their way out. Its sub-second sibling is written afterwards, as the array
	record id a table that declares nothing accepts. Runs as a request of its
	own so the removal is committed before the reads that import this.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ at: d'2024-01-01T00:00:00Z', id: r:[d'2024-01-01T00:00:00Z'], n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:[d'2024-01-01T00:00:00.500Z'], n: 2 }]"
*/

DEFINE TABLE r SCHEMALESS;
DEFINE FIELD id ON r TYPE [at: datetime];
CREATE r SET at = d'2024-01-01T00:00:00Z', n = 1;
REMOVE FIELD id ON r;
CREATE r:[d'2024-01-01T00:00:00.500Z'] SET n = 2;
