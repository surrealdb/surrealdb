/**
[test]
reason = "Test ORDER BY with Arabic characters (RTL text)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: '123' }, { value: 'ا' }, { value: 'العربية' }, { value: 'ب' }, { value: 'ت' }, { value: 'ث' }, { value: 'ج' }, { value: 'ح' }, { value: 'خ' }]"

[[test.results]]
value = "[{ value: 'خ' }, { value: 'ح' }, { value: 'ج' }, { value: 'ث' }, { value: 'ت' }, { value: 'ب' }, { value: 'العربية' }, { value: 'ا' }, { value: '123' }]"

[[test.results]]
value = "[{ value: 'ا' }, { value: '123' }, { value: 'ب' }, { value: 'ح' }, { value: 'ج' }, { value: 'خ' }, { value: 'العربية' }, { value: 'ت' }, { value: 'ث' }]"

[[test.results]]
value = "[{ value: 'ث' }, { value: 'ت' }, { value: 'العربية' }, { value: 'خ' }, { value: 'ج' }, { value: 'ح' }, { value: 'ب' }, { value: '123' }, { value: 'ا' }]"

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
SELECT `value` FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT `value` FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: Unicode-aware ordering
SELECT `value` FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT `value` FROM item ORDER BY value COLLATE DESC;
