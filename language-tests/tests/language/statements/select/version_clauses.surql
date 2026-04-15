/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause works correctly with WHERE, ORDER BY, LIMIT, and START clauses"

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
value = "[{ id: item:1, name: 'Alpha', score: 10 }, { id: item:2, name: 'Beta', score: 20 }, { id: item:3, name: 'Gamma', score: 30 }, { id: item:4, name: 'Delta', score: 40 }]"

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }, { id: item:2, name: 'Beta', score: 20 }, { id: item:3, name: 'Gamma', score: 30 }, { id: item:4, name: 'Delta', score: 40 }, { id: item:5, name: 'Epsilon', score: 50 }, { id: item:6, name: 'Zeta', score: 60 }]"

[[test.results]]
value = "[{ id: item:3, name: 'Gamma', score: 30 }, { id: item:4, name: 'Delta', score: 40 }]"

[[test.results]]
value = "[{ id: item:4, name: 'Delta', score: 40 }, { id: item:3, name: 'Gamma', score: 30 }, { id: item:2, name: 'Beta', score: 20 }, { id: item:1, name: 'Alpha', score: 10 }]"

[[test.results]]
value = "[{ id: item:1, name: 'Alpha', score: 10 }, { id: item:2, name: 'Beta', score: 20 }]"

[[test.results]]
value = "[{ id: item:3, name: 'Gamma', score: 30 }, { id: item:4, name: 'Delta', score: 40 }]"

[[test.results]]
value = "[{ id: item:4, name: 'Delta', score: 40 }, { id: item:3, name: 'Gamma', score: 30 }]"

*/

-- Create the initial batch of records
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	CREATE item:4 SET name = 'Delta', score = 40;
	RETURN "OK";
};

-- Capture a timestamp between the two batches
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- Create a second batch of records
{
	CREATE item:5 SET name = 'Epsilon', score = 50;
	CREATE item:6 SET name = 'Zeta', score = 60;
	RETURN "OK";
};

-- VERSION: basic temporal query returns only first batch
SELECT * FROM item VERSION $ts;

-- Current: all 6 records exist
SELECT * FROM item;

-- VERSION + WHERE: filtered temporal query
SELECT * FROM item WHERE score >= 30 VERSION $ts;

-- VERSION + ORDER BY DESC: ordered temporal query
SELECT * FROM item ORDER BY score DESC VERSION $ts;

-- VERSION + LIMIT: limited temporal query
SELECT * FROM item LIMIT 2 VERSION $ts;

-- VERSION + LIMIT + START: paginated temporal query
SELECT * FROM item LIMIT 2 START 2 VERSION $ts;

-- VERSION + WHERE + ORDER BY + LIMIT: combined clauses
SELECT * FROM item WHERE score >= 30 ORDER BY score DESC LIMIT 2 VERSION $ts;
