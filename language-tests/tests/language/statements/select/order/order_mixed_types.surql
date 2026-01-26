/**
[test]
reason = "Test ORDER BY when field has different types across records"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: false }, { id: item:2, value: 42 }, { id: item:3, value: 'text' }, { id: item:4, value: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: item:4, value: d'2024-01-01T00:00:00Z' }, { id: item:3, value: 'text' }, { id: item:2, value: 42 }, { id: item:1, value: false }]"
*/

-- Setup: Create records with different types in the same field
{
	CREATE item:1 SET value = false;
	CREATE item:2 SET value = 42;
	CREATE item:3 SET value = "text";
	CREATE item:4 SET value = d"2024-01-01T00:00:00Z";
	RETURN "OK";
};

-- Test ASC: Type ordering (bool < number < string < datetime)
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
