/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause should propagate through chained multi-hop graph traversals"
issue = 7245

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
value = "[{ hop2: [person:carol] }]"

[[test.results]]
value = "[{ hop2: [] }]"

*/

-- VERSION should propagate through chained graph traversals at every hop.
-- https://github.com/surrealdb/surrealdb/issues/7245
--
-- When multiple ->edge->table hops are chained, VERSION must apply to
-- each hop. Edges added after the timestamp should not appear at any depth.

-- Schema and initial single hop: alice -> bob
{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE follows TYPE RELATION IN person OUT person CHANGEFEED 1h;
	CREATE person:alice SET name = 'Alice';
	CREATE person:bob SET name = 'Bob';
	CREATE person:carol SET name = 'Carol';
	RELATE person:alice->follows->person:bob;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- After t1: bob now follows carol (creates a second hop from alice)
{
	RELATE person:bob->follows->person:carol;
	RETURN "OK";
};

-- HEAD depth-2: alice->bob->carol = [carol]
SELECT ->follows->person->follows->person AS hop2 FROM person:alice;

-- VERSION depth-2: at t1, bob did not yet follow carol, so hop2 is empty
SELECT ->follows->person->follows->person AS hop2 FROM person:alice VERSION $t1;
