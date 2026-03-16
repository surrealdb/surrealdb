/**
[test]
reason = "Test ORDER BY with mixed scripts in single strings"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: '123abc' }, { value: 'ABC' }, { value: 'abc123' }, { value: 'café日本' }, { value: 'hello世界' }, { value: 'Привет123' }, { value: 'مرحبا123' }, { value: '你好world' }]"

[[test.results]]
value = "[{ value: '你好world' }, { value: 'مرحبا123' }, { value: 'Привет123' }, { value: 'hello世界' }, { value: 'café日本' }, { value: 'abc123' }, { value: 'ABC' }, { value: '123abc' }]"

[[test.results]]
value = "[{ value: '123abc' }, { value: 'ABC' }, { value: 'abc123' }, { value: 'café日本' }, { value: 'hello世界' }, { value: 'مرحبا123' }, { value: '你好world' }, { value: 'Привет123' }]"

[[test.results]]
value = "[{ value: 'Привет123' }, { value: '你好world' }, { value: 'مرحبا123' }, { value: 'hello世界' }, { value: 'café日本' }, { value: 'abc123' }, { value: 'ABC' }, { value: '123abc' }]"

*/

-- Setup: Create records with mixed scripts (Latin, CJK, Cyrillic, Arabic, numbers)
{
	CREATE item:1 SET value = "ABC";
	CREATE item:2 SET value = "abc123";
	CREATE item:3 SET value = "hello世界";
	CREATE item:4 SET value = "café日本";
	CREATE item:5 SET value = "Привет123";
	CREATE item:6 SET value = "你好world";
	CREATE item:7 SET value = "123abc";
	CREATE item:8 SET value = "مرحبا123";
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
