/**
[test]
reason = "Test ORDER BY with Cyrillic characters (Russian alphabet)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'А' }, { value: 'Б' }, { value: 'В' }, { value: 'Г' }, { value: 'Д' }, { value: 'а' }, { value: 'б' }, { value: 'в' }, { value: 'г' }, { value: 'д' }]"

[[test.results]]
value = "[{ value: 'д' }, { value: 'г' }, { value: 'в' }, { value: 'б' }, { value: 'а' }, { value: 'Д' }, { value: 'Г' }, { value: 'В' }, { value: 'Б' }, { value: 'А' }]"

[[test.results]]
value = "[{ value: 'А' }, { value: 'а' }, { value: 'Б' }, { value: 'б' }, { value: 'Д' }, { value: 'д' }, { value: 'Г' }, { value: 'г' }, { value: 'В' }, { value: 'в' }]"

[[test.results]]
value = "[{ value: 'в' }, { value: 'В' }, { value: 'г' }, { value: 'Г' }, { value: 'д' }, { value: 'Д' }, { value: 'б' }, { value: 'Б' }, { value: 'а' }, { value: 'А' }]"

*/

-- Setup: Create records with Russian Cyrillic characters
{
	CREATE item:1 SET value = "А";
	CREATE item:2 SET value = "Б";
	CREATE item:3 SET value = "В";
	CREATE item:4 SET value = "Г";
	CREATE item:5 SET value = "Д";
	CREATE item:6 SET value = "а";
	CREATE item:7 SET value = "б";
	CREATE item:8 SET value = "в";
	CREATE item:9 SET value = "г";
	CREATE item:10 SET value = "д";
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
