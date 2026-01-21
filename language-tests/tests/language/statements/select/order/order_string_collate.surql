/**
[test]
reason = "Test ORDER BY with COLLATE for Unicode-aware string comparison"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:2, value: 'Apple' }, { id: item:3, value: 'BANANA' }, { id: item:1, value: 'apple' }, { id: item:4, value: 'cherry' }]"

[[test.results]]
value = "[{ id: item:4, value: 'cherry' }, { id: item:1, value: 'apple' }, { id: item:3, value: 'BANANA' }, { id: item:2, value: 'Apple' }]"

[[test.results]]
value = "[{ id: item:2, value: 'Apple' }, { id: item:1, value: 'apple' }, { id: item:3, value: 'BANANA' }, { id: item:4, value: 'cherry' }]"

[[test.results]]
value = "[{ id: item:4, value: 'cherry' }, { id: item:3, value: 'BANANA' }, { id: item:1, value: 'apple' }, { id: item:2, value: 'Apple' }]"

*/

-- Setup: Create records with strings of different cases
{
	CREATE item:1 SET value = "apple";
	CREATE item:2 SET value = "Apple";
	CREATE item:3 SET value = "BANANA";
	CREATE item:4 SET value = "cherry";
	RETURN "OK";
};

-- Test without COLLATE: case-sensitive (uppercase < lowercase)
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: case-insensitive lexical ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
