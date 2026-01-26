/**
[test]
reason = "Test ORDER BY with Thai script characters"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'ก' }, { value: 'ข' }, { value: 'ค' }, { value: 'ง' }, { value: 'จ' }, { value: 'ฉ' }, { value: 'ช' }, { value: 'ซ' }, { value: 'ภาษาไทย' }]"

[[test.results]]
value = "[{ value: 'ภาษาไทย' }, { value: 'ซ' }, { value: 'ช' }, { value: 'ฉ' }, { value: 'จ' }, { value: 'ง' }, { value: 'ค' }, { value: 'ข' }, { value: 'ก' }]"

[[test.results]]
value = "[{ value: 'จ' }, { value: 'ฉ' }, { value: 'ช' }, { value: 'ซ' }, { value: 'ก' }, { value: 'ข' }, { value: 'ค' }, { value: 'ง' }, { value: 'ภาษาไทย' }]"

[[test.results]]
value = "[{ value: 'ภาษาไทย' }, { value: 'ง' }, { value: 'ค' }, { value: 'ข' }, { value: 'ก' }, { value: 'ซ' }, { value: 'ช' }, { value: 'ฉ' }, { value: 'จ' }]"

*/

-- Setup: Create records with Thai script characters
{
	CREATE item:1 SET value = "ก";
	CREATE item:2 SET value = "ข";
	CREATE item:3 SET value = "ค";
	CREATE item:4 SET value = "ง";
	CREATE item:5 SET value = "จ";
	CREATE item:6 SET value = "ฉ";
	CREATE item:7 SET value = "ช";
	CREATE item:8 SET value = "ซ";
	CREATE item:9 SET value = "ภาษาไทย";
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
