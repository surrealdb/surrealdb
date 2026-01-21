/**
[test]
reason = "Test ORDER BY with German characters (umlauts and sharp s)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:3, value: 'Bar' }, { id: item:1, value: 'Bär' }, { id: item:4, value: 'Für' }, { id: item:2, value: 'bär' }, { id: item:5, value: 'für' }, { id: item:6, value: 'Über' }, { id: item:8, value: 'ß' }, { id: item:7, value: 'über' }]"

[[test.results]]
value = "[{ id: item:7, value: 'über' }, { id: item:8, value: 'ß' }, { id: item:6, value: 'Über' }, { id: item:5, value: 'für' }, { id: item:2, value: 'bär' }, { id: item:4, value: 'Für' }, { id: item:1, value: 'Bär' }, { id: item:3, value: 'Bar' }]"

[[test.results]]
value = "[{ id: item:3, value: 'Bar' }, { id: item:1, value: 'Bär' }, { id: item:2, value: 'bär' }, { id: item:4, value: 'Für' }, { id: item:5, value: 'für' }, { id: item:8, value: 'ß' }, { id: item:6, value: 'Über' }, { id: item:7, value: 'über' }]"

[[test.results]]
value = "[{ id: item:7, value: 'über' }, { id: item:6, value: 'Über' }, { id: item:8, value: 'ß' }, { id: item:5, value: 'für' }, { id: item:4, value: 'Für' }, { id: item:2, value: 'bär' }, { id: item:1, value: 'Bär' }, { id: item:3, value: 'Bar' }]"

*/

-- Setup: Create records with German characters (umlauts and sharp s)
{
	CREATE item:1 SET value = "Bär";
	CREATE item:2 SET value = "bär";
	CREATE item:3 SET value = "Bar";
	CREATE item:4 SET value = "Für";
	CREATE item:5 SET value = "für";
	CREATE item:6 SET value = "Über";
	CREATE item:7 SET value = "über";
	CREATE item:8 SET value = "ß";
	RETURN "OK";
};

-- Test without COLLATE ASC: case-sensitive (uppercase < lowercase)
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: case-insensitive lexical ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
