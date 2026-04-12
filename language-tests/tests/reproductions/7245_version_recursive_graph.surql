/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 5000

planner-strategy = ["all-ro"]

[test]
reason = "VERSION clause should propagate through recursive graph traversals"
issue = 7245

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ reachable: [person:bob, person:carol, person:dave] }]"

[[test.results]]
value = "[{ reachable: [person:bob, person:carol] }]"
*/

-- VERSION should propagate through recursive graph traversals
-- https://github.com/surrealdb/surrealdb/issues/7245
--
-- Multi-hop graph traversals (->edge->table repeated) should respect
-- VERSION at every hop. Edges and records added after the timestamp
-- should not appear at any depth.

-- Schema and initial chain: alice -> bob -> carol
{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE follows TYPE RELATION IN person OUT person CHANGEFEED 1h;
	CREATE person:alice SET name = 'Alice';
	CREATE person:bob SET name = 'Bob';
	CREATE person:carol SET name = 'Carol';
	RELATE person:alice->follows->person:bob;
	RELATE person:bob->follows->person:carol;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- After t1: extend the chain with dave
{
	CREATE person:dave SET name = 'Dave';
	RELATE person:carol->follows->person:dave;
	RETURN "OK";
};

-- HEAD: recursive reach from alice finds bob, carol, dave
SELECT ->follows->person.{1,5} AS reachable FROM person:alice;

-- VERSION: recursive reach at t1 finds only bob, carol (dave and the edge to dave didn't exist)
SELECT ->follows->person.{1,5} AS reachable FROM person:alice VERSION $t1;
