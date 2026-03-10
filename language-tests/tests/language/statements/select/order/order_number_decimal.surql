/**
[test]
reason = "Test ORDER BY with decimal values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: -99.99dec }, { id: item:2, value: 0.01dec }, { id: item:3, value: 10.50dec }, { id: item:4, value: 1000.001dec }]"

[[test.results]]
value = "[{ id: item:4, value: 1000.001dec }, { id: item:3, value: 10.50dec }, { id: item:2, value: 0.01dec }, { id: item:1, value: -99.99dec }]"
*/

-- Setup: Create records with decimal values
{
	CREATE item:1 SET value = -99.99dec;
	CREATE item:2 SET value = 0.01dec;
	CREATE item:3 SET value = 10.50dec;
	CREATE item:4 SET value = 1000.001dec;
	RETURN "OK";
};

-- Test ASC: smallest to largest
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: largest to smallest
SELECT * FROM item ORDER BY value DESC;
