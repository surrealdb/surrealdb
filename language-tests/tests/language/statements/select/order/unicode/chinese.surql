/**
[test]
reason = "Test ORDER BY with Chinese CJK ideographs"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: '世' }, { value: '中' }, { value: '你' }, { value: '国' }, { value: '好' }, { value: '文' }, { value: '汉' }, { value: '界' }]"

[[test.results]]
value = "[{ value: '界' }, { value: '汉' }, { value: '文' }, { value: '好' }, { value: '国' }, { value: '你' }, { value: '中' }, { value: '世' }]"

[[test.results]]
value = "[{ value: '国' }, { value: '好' }, { value: '界' }, { value: '你' }, { value: '世' }, { value: '文' }, { value: '汉' }, { value: '中' }]"

[[test.results]]
value = "[{ value: '中' }, { value: '汉' }, { value: '文' }, { value: '世' }, { value: '你' }, { value: '界' }, { value: '好' }, { value: '国' }]"

*/

-- Setup: Create records with Chinese CJK ideographs
{
	CREATE item:1 SET value = "世";
	CREATE item:2 SET value = "你";
	CREATE item:3 SET value = "好";
	CREATE item:4 SET value = "界";
	CREATE item:5 SET value = "中";
	CREATE item:6 SET value = "国";
	CREATE item:7 SET value = "文";
	CREATE item:8 SET value = "汉";
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
