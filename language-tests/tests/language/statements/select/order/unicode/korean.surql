/**
[test]
reason = "Test ORDER BY with Korean Hangul syllables"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'ABC' }, { value: '가' }, { value: '나' }, { value: '다' }, { value: '라' }, { value: '마' }, { value: '바' }, { value: '사' }, { value: '한국어' }]"

[[test.results]]
value = "[{ value: '한국어' }, { value: '사' }, { value: '바' }, { value: '마' }, { value: '라' }, { value: '다' }, { value: '나' }, { value: '가' }, { value: 'ABC' }]"

[[test.results]]
value = "[{ value: 'ABC' }, { value: '바' }, { value: '다' }, { value: '가' }, { value: '한국어' }, { value: '마' }, { value: '나' }, { value: '라' }, { value: '사' }]"

[[test.results]]
value = "[{ value: '사' }, { value: '라' }, { value: '나' }, { value: '마' }, { value: '한국어' }, { value: '가' }, { value: '다' }, { value: '바' }, { value: 'ABC' }]"

*/

-- Setup: Create records with Korean Hangul syllables mixed with ASCII
{
	CREATE item:1 SET value = "가";
	CREATE item:2 SET value = "나";
	CREATE item:3 SET value = "다";
	CREATE item:4 SET value = "라";
	CREATE item:5 SET value = "마";
	CREATE item:6 SET value = "바";
	CREATE item:7 SET value = "사";
	CREATE item:8 SET value = "ABC";
	CREATE item:9 SET value = "한국어";
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
