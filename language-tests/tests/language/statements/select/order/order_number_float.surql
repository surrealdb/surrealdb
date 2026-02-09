/**
[test]
reason = "Test ORDER BY with float values including special values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: -3.14f }, { id: item:2, value: 0f }, { id: item:3, value: 2.718f }, { id: item:4, value: 42.5f }]"

[[test.results]]
value = "[{ id: item:4, value: 42.5f }, { id: item:3, value: 2.718f }, { id: item:2, value: 0f }, { id: item:1, value: -3.14f }]"
*/

-- Setup: Create records with float values
{
	CREATE item:1 SET value = -3.14f;
	CREATE item:2 SET value = 0.0f;
	CREATE item:3 SET value = 2.718f;
	CREATE item:4 SET value = 42.5f;
	RETURN "OK";
};

-- Test ASC: smallest to largest
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: largest to smallest
SELECT * FROM item ORDER BY value DESC;
