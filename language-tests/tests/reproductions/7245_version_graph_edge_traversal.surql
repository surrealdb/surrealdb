/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause should propagate through graph edge traversals"
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
value = "[{ friends: ['Bob', 'Carol'] }]"

[[test.results]]
value = "[{ friends: ['Bob'] }]"

[[test.results]]
value = "[{ name: 'Alice' }]"
*/

-- Reproduction test for issue #7245: VERSION clause does not propagate through graph edge traversals
-- https://github.com/surrealdb/surrealdb/issues/7245
--
-- When a query uses ->edge->table path expressions, the traversal should
-- honour the VERSION timestamp and only return edges that existed at that
-- point in time. Previously, graph traversal always returned HEAD edges.

-- Schema
{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE knows TYPE RELATION IN person OUT person CHANGEFEED 1h;
	CREATE person:alice SET name = 'Alice';
	CREATE person:bob SET name = 'Bob';
	RELATE person:alice->knows->person:bob SET since = 2020;
	RETURN "OK";
};

-- Capture a timestamp after the first edge exists
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- Add carol, alice now knows carol too, and mutate alice's field
{
	CREATE person:carol SET name = 'Carol';
	RELATE person:alice->knows->person:carol SET since = 2024;
	UPDATE person:alice SET name = 'Alice (updated)';
	RETURN "OK";
};

-- Query 1: current state — both edges visible
SELECT array::sort(->knows->person.name) AS friends FROM person:alice;

-- Query 2: VERSION should traverse only edges that existed at $t1
SELECT array::sort(->knows->person.name) AS friends FROM person:alice VERSION $t1;

-- Query 3: sanity check — VERSION works for direct fields
SELECT name FROM person:alice VERSION $t1;
