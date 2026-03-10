/**
[test]
reason = "Test ORDER BY with string values (case-sensitive)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'Apple' }, { id: item:2, value: 'Banana' }, { id: item:3, value: 'apple' }, { id: item:4, value: 'cherry' }]"

[[test.results]]
value = "[{ id: item:4, value: 'cherry' }, { id: item:3, value: 'apple' }, { id: item:2, value: 'Banana' }, { id: item:1, value: 'Apple' }]"
*/

-- Setup: Create records with string values
{
	CREATE item:1 SET value = "Apple";
	CREATE item:2 SET value = "Banana";
	CREATE item:3 SET value = "apple";
	CREATE item:4 SET value = "cherry";
	RETURN "OK";
};

-- Test ASC: case-sensitive ordering (uppercase before lowercase)
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
