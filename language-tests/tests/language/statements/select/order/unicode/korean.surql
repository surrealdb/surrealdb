/**
[test]
reason = "Test ORDER BY with Korean Hangul syllables"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:8, value: 'ABC' }, { id: item:1, value: '가' }, { id: item:2, value: '나' }, { id: item:3, value: '다' }, { id: item:4, value: '라' }, { id: item:5, value: '마' }, { id: item:6, value: '바' }, { id: item:7, value: '사' }, { id: item:9, value: '한국어' }]"

[[test.results]]
value = "[{ id: item:9, value: '한국어' }, { id: item:7, value: '사' }, { id: item:6, value: '바' }, { id: item:5, value: '마' }, { id: item:4, value: '라' }, { id: item:3, value: '다' }, { id: item:2, value: '나' }, { id: item:1, value: '가' }, { id: item:8, value: 'ABC' }]"

[[test.results]]
value = "[{ id: item:8, value: 'ABC' }, { id: item:6, value: '바' }, { id: item:3, value: '다' }, { id: item:1, value: '가' }, { id: item:9, value: '한국어' }, { id: item:5, value: '마' }, { id: item:2, value: '나' }, { id: item:4, value: '라' }, { id: item:7, value: '사' }]"

[[test.results]]
value = "[{ id: item:7, value: '사' }, { id: item:4, value: '라' }, { id: item:2, value: '나' }, { id: item:5, value: '마' }, { id: item:9, value: '한국어' }, { id: item:1, value: '가' }, { id: item:3, value: '다' }, { id: item:6, value: '바' }, { id: item:8, value: 'ABC' }]"
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
SELECT * FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT * FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: Unicode-aware ordering
SELECT * FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT * FROM item ORDER BY value COLLATE DESC;
