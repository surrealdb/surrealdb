/**
[test]
reason = "Test ORDER BY with accented Latin characters (French, Spanish, Nordic)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'cafe' }, { id: item:2, value: 'café' }, { id: item:3, value: 'à' }, { id: item:9, value: 'å' }, { id: item:10, value: 'æ' }, { id: item:7, value: 'ç' }, { id: item:4, value: 'è' }, { id: item:5, value: 'é' }, { id: item:6, value: 'ê' }, { id: item:8, value: 'ñ' }, { id: item:11, value: 'ø' }]"

[[test.results]]
value = "[{ id: item:11, value: 'ø' }, { id: item:8, value: 'ñ' }, { id: item:6, value: 'ê' }, { id: item:5, value: 'é' }, { id: item:4, value: 'è' }, { id: item:7, value: 'ç' }, { id: item:10, value: 'æ' }, { id: item:9, value: 'å' }, { id: item:3, value: 'à' }, { id: item:2, value: 'café' }, { id: item:1, value: 'cafe' }]"

[[test.results]]
value = "[{ id: item:3, value: 'à' }, { id: item:9, value: 'å' }, { id: item:10, value: 'æ' }, { id: item:7, value: 'ç' }, { id: item:1, value: 'cafe' }, { id: item:2, value: 'café' }, { id: item:4, value: 'è' }, { id: item:5, value: 'é' }, { id: item:6, value: 'ê' }, { id: item:8, value: 'ñ' }, { id: item:11, value: 'ø' }]"

[[test.results]]
value = "[{ id: item:11, value: 'ø' }, { id: item:8, value: 'ñ' }, { id: item:6, value: 'ê' }, { id: item:5, value: 'é' }, { id: item:4, value: 'è' }, { id: item:2, value: 'café' }, { id: item:1, value: 'cafe' }, { id: item:7, value: 'ç' }, { id: item:10, value: 'æ' }, { id: item:9, value: 'å' }, { id: item:3, value: 'à' }]"

*/

-- Setup: Create records with accented Latin characters from French, Spanish, and Nordic languages
{
	CREATE item:1 SET value = "cafe";
	CREATE item:2 SET value = "café";
	CREATE item:3 SET value = "à";
	CREATE item:4 SET value = "è";
	CREATE item:5 SET value = "é";
	CREATE item:6 SET value = "ê";
	CREATE item:7 SET value = "ç";
	CREATE item:8 SET value = "ñ";
	CREATE item:9 SET value = "å";
	CREATE item:10 SET value = "æ";
	CREATE item:11 SET value = "ø";
	RETURN "OK";
};

-- Test without COLLATE ASC: Unicode code point ordering
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: Unicode-aware ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
