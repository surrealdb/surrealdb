/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION should propagate from outer SELECT into subquery table scans"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: person:1, pets: [{ id: pet:1, name: 'Rex', owner: person:1 }, { id: pet:2, name: 'Spot', owner: person:1 }] }]"

[[test.results]]
value = "[{ id: person:1, pets: [{ id: pet:1, name: 'Rex', owner: person:1 }] }]"

[[test.results]]
value = "[{ id: person:1, pets: [{ id: pet:1, name: 'Rex', owner: person:1 }] }]"

*/

-- Subquery table scans should inherit VERSION from the outer SELECT.
-- This tests the ctx.version_stamp() fallback on DynamicScan/TableScan.

{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE pet SCHEMALESS CHANGEFEED 1h;
	CREATE person:1;
	CREATE pet:1 SET name = 'Rex', owner = person:1;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

{
	CREATE pet:2 SET name = 'Spot', owner = person:1;
	RETURN "OK";
};

-- HEAD: subquery sees both pets
SELECT *, (SELECT * FROM pet WHERE owner = $parent.id) AS pets FROM person:1;

-- VERSION on outer only: subquery should inherit version and see only Rex
SELECT *, (SELECT * FROM pet WHERE owner = $parent.id) AS pets FROM person:1 VERSION $t1;

-- VERSION on both: explicit inner version should match
SELECT *, (SELECT * FROM pet WHERE owner = $parent.id VERSION $t1) AS pets FROM person:1 VERSION $t1;
