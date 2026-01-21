/**
[test]
reason = "Test ORDER BY with Cyrillic characters (Russian alphabet)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'А' }, { id: item:2, value: 'Б' }, { id: item:3, value: 'В' }, { id: item:4, value: 'Г' }, { id: item:5, value: 'Д' }, { id: item:6, value: 'а' }, { id: item:7, value: 'б' }, { id: item:8, value: 'в' }, { id: item:9, value: 'г' }, { id: item:10, value: 'д' }]"

[[test.results]]
value = "[{ id: item:10, value: 'д' }, { id: item:9, value: 'г' }, { id: item:8, value: 'в' }, { id: item:7, value: 'б' }, { id: item:6, value: 'а' }, { id: item:5, value: 'Д' }, { id: item:4, value: 'Г' }, { id: item:3, value: 'В' }, { id: item:2, value: 'Б' }, { id: item:1, value: 'А' }]"

[[test.results]]
value = "[{ id: item:1, value: 'А' }, { id: item:6, value: 'а' }, { id: item:2, value: 'Б' }, { id: item:7, value: 'б' }, { id: item:5, value: 'Д' }, { id: item:10, value: 'д' }, { id: item:4, value: 'Г' }, { id: item:9, value: 'г' }, { id: item:3, value: 'В' }, { id: item:8, value: 'в' }]"

[[test.results]]
value = "[{ id: item:8, value: 'в' }, { id: item:3, value: 'В' }, { id: item:9, value: 'г' }, { id: item:4, value: 'Г' }, { id: item:10, value: 'д' }, { id: item:5, value: 'Д' }, { id: item:7, value: 'б' }, { id: item:2, value: 'Б' }, { id: item:6, value: 'а' }, { id: item:1, value: 'А' }]"
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
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: case-insensitive lexical ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
