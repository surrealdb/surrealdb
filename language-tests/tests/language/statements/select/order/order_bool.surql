/**
[test]
reason = "Test ORDER BY with boolean values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: false }, { id: item:3, value: false }, { id: item:2, value: true }, { id: item:4, value: true }]"

[[test.results]]
value = "[{ id: item:2, value: true }, { id: item:4, value: true }, { id: item:1, value: false }, { id: item:3, value: false }]"
*/

-- Setup: Create records with boolean values
{
	CREATE item:1 SET value = false;
	CREATE item:2 SET value = true;
	CREATE item:3 SET value = false;
	CREATE item:4 SET value = true;
	RETURN "OK";
};

-- Test ASC: false < true
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: true > false
SELECT * FROM item ORDER BY value DESC;
