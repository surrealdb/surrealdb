/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause should propagate through FETCH and record field dereferences"
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
value = "[{ address: address:home, city: 'London' }]"

[[test.results]]
value = "[{ address: address:home, city: 'Paris' }]"

[[test.results]]
value = "[{ address: { city: 'London', id: address:home }, id: person:alice }]"

[[test.results]]
value = "[{ address: { city: 'Paris', id: address:home }, id: person:alice }]"

*/

-- VERSION clause should propagate through FETCH and record field dereferences
-- https://github.com/surrealdb/surrealdb/issues/7245
--
-- When a SELECT uses VERSION, the FETCH clause and field auto-dereferences
-- (accessing a field on a RecordId) should also read at the versioned timestamp.

-- Schema and initial data
{
	DEFINE TABLE person SCHEMALESS CHANGEFEED 1h;
	DEFINE TABLE address SCHEMALESS CHANGEFEED 1h;
	CREATE address:home SET city = 'Paris';
	CREATE person:alice SET address = address:home;
	RETURN "OK";
};

-- Capture timestamp
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- Update the address after the timestamp
{
	UPDATE address:home SET city = 'London';
	RETURN "OK";
};

-- Query 1: HEAD — field dereference should show London
SELECT address, address.city AS city FROM person:alice;

-- Query 2: VERSION — field dereference should show Paris (the value at $t1)
SELECT address, address.city AS city FROM person:alice VERSION $t1;

-- Query 3: HEAD — FETCH should resolve to London
SELECT * FROM person:alice FETCH address;

-- Query 4: VERSION — FETCH should resolve to Paris (the value at $t1)
SELECT * FROM person:alice FETCH address VERSION $t1;
