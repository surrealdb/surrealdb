/**
[test]
reason = "Test ORDER BY with Arabic characters (RTL text)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:9, value: '123' }, { id: item:1, value: 'ا' }, { id: item:8, value: 'العربية' }, { id: item:2, value: 'ب' }, { id: item:3, value: 'ت' }, { id: item:4, value: 'ث' }, { id: item:5, value: 'ج' }, { id: item:6, value: 'ح' }, { id: item:7, value: 'خ' }]"

[[test.results]]
value = "[{ id: item:7, value: 'خ' }, { id: item:6, value: 'ح' }, { id: item:5, value: 'ج' }, { id: item:4, value: 'ث' }, { id: item:3, value: 'ت' }, { id: item:2, value: 'ب' }, { id: item:8, value: 'العربية' }, { id: item:1, value: 'ا' }, { id: item:9, value: '123' }]"

[[test.results]]
value = "[{ id: item:1, value: 'ا' }, { id: item:9, value: '123' }, { id: item:2, value: 'ب' }, { id: item:6, value: 'ح' }, { id: item:5, value: 'ج' }, { id: item:7, value: 'خ' }, { id: item:8, value: 'العربية' }, { id: item:3, value: 'ت' }, { id: item:4, value: 'ث' }]"

[[test.results]]
value = "[{ id: item:4, value: 'ث' }, { id: item:3, value: 'ت' }, { id: item:8, value: 'العربية' }, { id: item:7, value: 'خ' }, { id: item:5, value: 'ج' }, { id: item:6, value: 'ح' }, { id: item:2, value: 'ب' }, { id: item:9, value: '123' }, { id: item:1, value: 'ا' }]"

*/

-- Setup: Create records with Arabic characters (right-to-left text)
{
	CREATE item:1 SET value = "ا";
	CREATE item:2 SET value = "ب";
	CREATE item:3 SET value = "ت";
	CREATE item:4 SET value = "ث";
	CREATE item:5 SET value = "ج";
	CREATE item:6 SET value = "ح";
	CREATE item:7 SET value = "خ";
	CREATE item:8 SET value = "العربية";
	CREATE item:9 SET value = "123";
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
