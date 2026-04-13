/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause should propagate through graph traversal combined with FETCH and nested dereference"
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
value = "[{ friends: [{ address: address:hq, id: person:bob, name: 'Bob (updated)' }] }]"

[[test.results]]
value = "[{ friends: [{ address: address:hq, id: person:bob, name: 'Bob' }] }]"

[[test.results]]
value = "[{ friends: [{ address: { city: 'London', id: address:hq }, id: person:bob, name: 'Bob (updated)' }] }]"

[[test.results]]
value = "[{ friends: [{ address: { city: 'Paris', id: address:hq }, id: person:bob, name: 'Bob' }] }]"

*/

-- VERSION should propagate through graph traversal, FETCH, and nested dereference together
-- https://github.com/surrealdb/surrealdb/issues/7245
--
-- Tests the full combination: graph edges, the target record's fields, and
-- linked records from those targets (via FETCH) should all read at the
-- same VERSION timestamp.

-- Schema and initial data
{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE address SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE knows TYPE RELATION IN person OUT person CHANGEFEED 1h;
	CREATE address:hq SET city = 'Paris';
	CREATE person:alice SET name = 'Alice';
	CREATE person:bob SET name = 'Bob', address = address:hq;
	RELATE person:alice->knows->person:bob;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- After t1: update bob's name and the linked address
{
	UPDATE person:bob SET name = 'Bob (updated)';
	UPDATE address:hq SET city = 'London';
	RETURN "OK";
};

-- HEAD: graph + field dereference — sees updated name and London
SELECT ->knows->person.* AS friends FROM person:alice;

-- VERSION: graph + field dereference — original name and Paris
SELECT ->knows->person.* AS friends FROM person:alice VERSION $t1;

-- HEAD: graph + FETCH — resolves address link to London
SELECT ->knows->person.* AS friends FROM person:alice FETCH friends.address;

-- VERSION: graph + FETCH — resolves address link to Paris
SELECT ->knows->person.* AS friends FROM person:alice FETCH friends.address VERSION $t1;
