/**
[test]
reason = "Test ORDER BY with German characters (umlauts and sharp s)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'Bar' }, { value: 'Bär' }, { value: 'Für' }, { value: 'bär' }, { value: 'für' }, { value: 'ss' }, { value: 'Über' }, { value: 'ß' }, { value: 'über' }]"

[[test.results]]
value = "[{ value: 'über' }, { value: 'ß' }, { value: 'Über' }, { value: 'ss' }, { value: 'für' }, { value: 'bär' }, { value: 'Für' }, { value: 'Bär' }, { value: 'Bar' }]"

[[test.results]]
value = "[{ value: 'Bar' }, { value: 'Bär' }, { value: 'bär' }, { value: 'Für' }, { value: 'für' }, { value: 'ss' }, { value: 'ß' }, { value: 'Über' }, { value: 'über' }]"

[[test.results]]
value = "[{ value: 'über' }, { value: 'Über' }, { value: 'ß' }, { value: 'ss' }, { value: 'für' }, { value: 'Für' }, { value: 'bär' }, { value: 'Bär' }, { value: 'Bar' }]"

*/

-- Setup: Create records with German characters (umlauts and sharp s)
{
	CREATE item:1 SET value = "Bär";
	CREATE item:2 SET value = "bär";
	CREATE item:3 SET value = "Bar";
	CREATE item:4 SET value = "Für";
	CREATE item:5 SET value = "für";
	CREATE item:6 SET value = "ss";
	CREATE item:7 SET value = "Über";
	CREATE item:8 SET value = "über";
	CREATE item:9 SET value = "ß";
	RETURN "OK";
};

-- Test without COLLATE ASC: case-sensitive (uppercase < lowercase)
SELECT `value` FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT `value` FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: case-insensitive lexical ordering
SELECT `value` FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT `value` FROM item ORDER BY value COLLATE DESC;
