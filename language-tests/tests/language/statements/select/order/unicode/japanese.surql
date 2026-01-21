/**
[test]
reason = "Test ORDER BY with Japanese characters (hiragana, katakana, kanji)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 'あ' }, { id: item:2, value: 'い' }, { id: item:3, value: 'う' }, { id: item:4, value: 'え' }, { id: item:5, value: 'お' }, { id: item:6, value: 'ア' }, { id: item:7, value: 'イ' }, { id: item:8, value: 'ウ' }, { id: item:9, value: '日' }, { id: item:10, value: '本' }, { id: item:11, value: '語' }]"

[[test.results]]
value = "[{ id: item:11, value: '語' }, { id: item:10, value: '本' }, { id: item:9, value: '日' }, { id: item:8, value: 'ウ' }, { id: item:7, value: 'イ' }, { id: item:6, value: 'ア' }, { id: item:5, value: 'お' }, { id: item:4, value: 'え' }, { id: item:3, value: 'う' }, { id: item:2, value: 'い' }, { id: item:1, value: 'あ' }]"

[[test.results]]
value = "[{ id: item:1, value: 'あ' }, { id: item:6, value: 'ア' }, { id: item:10, value: '本' }, { id: item:4, value: 'え' }, { id: item:2, value: 'い' }, { id: item:7, value: 'イ' }, { id: item:5, value: 'お' }, { id: item:9, value: '日' }, { id: item:3, value: 'う' }, { id: item:8, value: 'ウ' }, { id: item:11, value: '語' }]"

[[test.results]]
value = "[{ id: item:11, value: '語' }, { id: item:8, value: 'ウ' }, { id: item:3, value: 'う' }, { id: item:9, value: '日' }, { id: item:5, value: 'お' }, { id: item:7, value: 'イ' }, { id: item:2, value: 'い' }, { id: item:4, value: 'え' }, { id: item:10, value: '本' }, { id: item:6, value: 'ア' }, { id: item:1, value: 'あ' }]"

*/

-- Setup: Create records with Japanese hiragana, katakana, and kanji
{
	CREATE item:1 SET value = "あ";
	CREATE item:2 SET value = "い";
	CREATE item:3 SET value = "う";
	CREATE item:4 SET value = "え";
	CREATE item:5 SET value = "お";
	CREATE item:6 SET value = "ア";
	CREATE item:7 SET value = "イ";
	CREATE item:8 SET value = "ウ";
	CREATE item:9 SET value = "日";
	CREATE item:10 SET value = "本";
	CREATE item:11 SET value = "語";
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
