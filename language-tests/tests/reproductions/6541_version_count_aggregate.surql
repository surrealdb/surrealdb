/**
[env]
backend = ["surrealkv+versioned"]
timeout = 5000

[test]
reason = "VERSION clause with aggregate functions should return correct results matching a plain SELECT with VERSION"
issue = 6541

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
value = "[{ id: something:1 }, { id: something:2 }, { id: something:3 }, { id: something:4 }, { id: something:5 }, { id: something:6 }, { id: something:7 }]"

[[test.results]]
value = "[{ id: something:1 }, { id: something:2 }, { id: something:3 }, { id: something:4 }]"

[[test.results]]
value = "[{ count: 7 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 7 }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 4 }]"
*/

-- Reproduction test for issue #6541: VERSION clause with aggregate functions
-- https://github.com/surrealdb/surrealdb/issues/6541
--
-- When using VERSION with GROUP BY ALL at the main statement level, the count
-- should match the number of records that existed at that point in time,
-- not the total number of records in the table.

-- Create the first batch of records
{
	CREATE something:1;
	CREATE something:2;
	CREATE something:3;
	CREATE something:4;
	RETURN "OK";
};

-- Sleep to create a time gap, then capture the timestamp
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- Create the second batch of records
{
	CREATE something:5;
	CREATE something:6;
	CREATE something:7;
	RETURN "OK";
};

-- Query 1: Select all records at the latest timestamp
SELECT * FROM something;

-- Query 2: Basic temporal query should return only the first 4 records
SELECT * FROM something VERSION $ts;

-- Query 3: GROUP ALL with no VERSION should count all records
SELECT count() FROM something GROUP ALL;

-- Query 4: VERSION with GROUP ALL should count at the specified version
SELECT count() FROM something GROUP ALL VERSION $ts;

-- Query 5: VERSION with GROUP ALL should count at the specified version
SELECT count() FROM something GROUP ALL VERSION "1970-01-01T00:00:00Z";

-- Query 6: VERSION with GROUP ALL and no VERSION in subquery should use the latest version
SELECT count() FROM (SELECT * FROM something) GROUP ALL VERSION $ts;

-- Query 7: VERSION in subquery should be used to retrieve the records
SELECT count() FROM (SELECT * FROM something VERSION $ts) GROUP ALL;

-- Query 8: VERSION in subquery should be used instead of VERSION from outer statement
SELECT count() FROM (SELECT * FROM something VERSION $ts, something VERSION ) GROUP ALL;
