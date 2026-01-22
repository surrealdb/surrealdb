/**
[test]
reason = "Test ORDER BY rand() produces random ordering"

[[test.results]]
value = "'OK'"

[[test.results]]
match = "array::len($result) == 5"
error = false

[[test.results]]
match = "array::len($result) == 5"
error = false

[[test.results]]
match = "array::len($result) == 5"
error = false
*/

-- Setup: Create multiple records
{
	CREATE item:1 SET value = 1;
	CREATE item:2 SET value = 2;
	CREATE item:3 SET value = 3;
	CREATE item:4 SET value = 4;
	CREATE item:5 SET value = 5;
	RETURN "OK";
};

-- Test: ORDER BY rand() returns all records (order may vary)
SELECT * FROM item ORDER BY rand();

-- Test: Multiple calls may produce different orders
SELECT * FROM item ORDER BY rand();

-- Test: Can be combined with LIMIT
SELECT * FROM item ORDER BY rand() LIMIT 5;
