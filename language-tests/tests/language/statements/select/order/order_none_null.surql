/**
[test]
reason = "Test ORDER BY with NONE and NULL values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1 }, { id: item:2, value: NULL }, { id: item:3, value: false }, { id: item:4, value: 'text' }]"

[[test.results]]
value = "[{ id: item:4, value: 'text' }, { id: item:3, value: false }, { id: item:2, value: NULL }, { id: item:1 }]"
*/

-- Setup: Create records with NONE (missing field), NULL, and other values
{
	CREATE item:1;
	CREATE item:2 SET value = NULL;
	CREATE item:3 SET value = false;
	CREATE item:4 SET value = "text";
	RETURN "OK";
};

-- Test ASC: NONE < NULL < other values
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
