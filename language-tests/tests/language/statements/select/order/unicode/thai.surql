/**
[test]
reason = "Test ORDER BY with Thai script characters"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'ก' }, { id: item:2, value: 'ข' }, { id: item:3, value: 'ค' }, { id: item:4, value: 'ง' }, { id: item:5, value: 'จ' }, { id: item:6, value: 'ฉ' }, { id: item:7, value: 'ช' }, { id: item:8, value: 'ซ' }, { id: item:9, value: 'ภาษาไทย' }]"

[[test.results]]
value = "[{ id: item:9, value: 'ภาษาไทย' }, { id: item:8, value: 'ซ' }, { id: item:7, value: 'ช' }, { id: item:6, value: 'ฉ' }, { id: item:5, value: 'จ' }, { id: item:4, value: 'ง' }, { id: item:3, value: 'ค' }, { id: item:2, value: 'ข' }, { id: item:1, value: 'ก' }]"

[[test.results]]
value = "[{ id: item:5, value: 'จ' }, { id: item:6, value: 'ฉ' }, { id: item:7, value: 'ช' }, { id: item:8, value: 'ซ' }, { id: item:1, value: 'ก' }, { id: item:2, value: 'ข' }, { id: item:3, value: 'ค' }, { id: item:4, value: 'ง' }, { id: item:9, value: 'ภาษาไทย' }]"

[[test.results]]
value = "[{ id: item:9, value: 'ภาษาไทย' }, { id: item:4, value: 'ง' }, { id: item:3, value: 'ค' }, { id: item:2, value: 'ข' }, { id: item:1, value: 'ก' }, { id: item:8, value: 'ซ' }, { id: item:7, value: 'ช' }, { id: item:6, value: 'ฉ' }, { id: item:5, value: 'จ' }]"

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
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: Unicode-aware ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
