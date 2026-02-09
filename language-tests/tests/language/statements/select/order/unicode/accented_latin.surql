/**
[test]
reason = "Test ORDER BY with accented Latin characters (French, Spanish, Nordic)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'cafe' }, { value: 'café' }, { value: 'à' }, { value: 'å' }, { value: 'æ' }, { value: 'ç' }, { value: 'è' }, { value: 'é' }, { value: 'ê' }, { value: 'ñ' }, { value: 'ø' }]"

[[test.results]]
value = "[{ value: 'ø' }, { value: 'ñ' }, { value: 'ê' }, { value: 'é' }, { value: 'è' }, { value: 'ç' }, { value: 'æ' }, { value: 'å' }, { value: 'à' }, { value: 'café' }, { value: 'cafe' }]"

[[test.results]]
value = "[{ value: 'à' }, { value: 'å' }, { value: 'æ' }, { value: 'ç' }, { value: 'cafe' }, { value: 'café' }, { value: 'è' }, { value: 'é' }, { value: 'ê' }, { value: 'ñ' }, { value: 'ø' }]"

[[test.results]]
value = "[{ value: 'ø' }, { value: 'ñ' }, { value: 'ê' }, { value: 'é' }, { value: 'è' }, { value: 'café' }, { value: 'cafe' }, { value: 'ç' }, { value: 'æ' }, { value: 'å' }, { value: 'à' }]"

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
SELECT `value` FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT `value` FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: Unicode-aware ordering
SELECT `value` FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT `value` FROM item ORDER BY value COLLATE DESC;
