/**
[test]
reason = "Test ORDER BY with integer values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: -100 }, { id: item:2, value: 0 }, { id: item:3, value: 42 }, { id: item:4, value: 1000 }]"

[[test.results]]
value = "[{ id: item:4, value: 1000 }, { id: item:3, value: 42 }, { id: item:2, value: 0 }, { id: item:1, value: -100 }]"
*/

-- Setup: Create records with integer values
{
	CREATE item:1 SET value = -100;
	CREATE item:2 SET value = 0;
	CREATE item:3 SET value = 42;
	CREATE item:4 SET value = 1000;
	RETURN "OK";
};

-- Test ASC: smallest to largest
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: largest to smallest
SELECT * FROM item ORDER BY value DESC;
