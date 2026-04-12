/**
[env]
backend = ["mem", "surrealkv"]
versioned = true
timeout = 5000

planner-strategy = ["all-ro"]

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
value = "[{ friends: [{ address: address:hq, id: person:carol, name: 'Carol' }, { address: address:hq, id: person:bob, name: 'Bob (updated)' }] }]"

[[test.results]]
value = "[{ friends: [{ address: address:hq, id: person:bob, name: 'Bob' }] }]"

[[test.results]]
value = "[{ friends: [{ address: { city: 'London', id: address:hq }, id: person:carol, name: 'Carol' }, { address: { city: 'London', id: address:hq }, id: person:bob, name: 'Bob (updated)' }] }]"

[[test.results]]
value = "[{ friends: [{ address: { city: 'Paris', id: address:hq }, id: person:bob, name: 'Bob' }] }]"

*/

-- VERSION should propagate through graph traversal, FETCH, and nested dereference together
-- https://github.com/surrealdb/surrealdb/issues/7245
--
-- Tests the full combination: graph edges, the target records, linked records
-- from those targets (via FETCH), and field dereferences should all read at the
-- same VERSION timestamp.

-- Schema and initial data
{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE address SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE knows TYPE RELATION IN person OUT person CHANGEFEED 1h;
	CREATE address:hq SET city = 'Paris';
	CREATE person:alice SET name = 'Alice', address = address:hq;
	CREATE person:bob SET name = 'Bob', address = address:hq;
	RELATE person:alice->knows->person:bob;
	RETURN "OK";
};

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- After t1: add carol, update bob, update address
{
	CREATE person:carol SET name = 'Carol', address = address:hq;
	RELATE person:alice->knows->person:carol;
	UPDATE person:bob SET name = 'Bob (updated)';
	UPDATE address:hq SET city = 'London';
	RETURN "OK";
};

-- HEAD: graph + field dereference — sees both edges, updated name, London
SELECT ->knows->person.* AS friends FROM person:alice;

-- VERSION: graph + field dereference — only bob edge, original name, Paris
SELECT ->knows->person.* AS friends FROM person:alice VERSION $t1;

-- HEAD: graph + FETCH — resolves address link to London
SELECT ->knows->person.* AS friends FROM person:alice FETCH friends.address;

-- VERSION: graph + FETCH — resolves address link to Paris
SELECT ->knows->person.* AS friends FROM person:alice FETCH friends.address VERSION $t1;
