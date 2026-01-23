/**
[test]
reason = "Test ORDER BY with emoji characters (Unicode code point ordering)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: '⭐' }, { value: '🐭' }, { value: '🐱' }, { value: '🐶' }, { value: '💎' }, { value: '🔥' }, { value: '😀' }, { value: '😃' }, { value: '😊' }]"

[[test.results]]
value = "[{ value: '😊' }, { value: '😃' }, { value: '😀' }, { value: '🔥' }, { value: '💎' }, { value: '🐶' }, { value: '🐱' }, { value: '🐭' }, { value: '⭐' }]"

[[test.results]]
value = "[{ value: '⭐' }, { value: '🐭' }, { value: '🐱' }, { value: '🐶' }, { value: '💎' }, { value: '🔥' }, { value: '😀' }, { value: '😃' }, { value: '😊' }]"

[[test.results]]
value = "[{ value: '😊' }, { value: '😃' }, { value: '😀' }, { value: '🔥' }, { value: '💎' }, { value: '🐶' }, { value: '🐱' }, { value: '🐭' }, { value: '⭐' }]"

*/

-- Setup: Create records with emoji characters from different categories
{
	CREATE item:1 SET value = "😀";
	CREATE item:2 SET value = "😃";
	CREATE item:3 SET value = "😊";
	CREATE item:4 SET value = "🐱";
	CREATE item:5 SET value = "🐶";
	CREATE item:6 SET value = "🐭";
	CREATE item:7 SET value = "🔥";
	CREATE item:8 SET value = "⭐";
	CREATE item:9 SET value = "💎";
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
