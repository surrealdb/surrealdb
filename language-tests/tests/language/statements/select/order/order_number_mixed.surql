/**
[test]
reason = "Test ORDER BY with mixed number types (int, float, decimal)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: -10 }, { id: item:2, value: 0.5f }, { id: item:3, value: 5dec }, { id: item:4, value: 100 }]"

[[test.results]]
value = "[{ id: item:4, value: 100 }, { id: item:3, value: 5dec }, { id: item:2, value: 0.5f }, { id: item:1, value: -10 }]"
*/

-- Setup: Create records with mixed number types
{
	CREATE item:1 SET value = -10;
	CREATE item:2 SET value = 0.5f;
	CREATE item:3 SET value = 5.0dec;
	CREATE item:4 SET value = 100;
	RETURN "OK";
};

-- Test ASC: smallest to largest across types
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: largest to smallest across types
SELECT * FROM item ORDER BY value DESC;
