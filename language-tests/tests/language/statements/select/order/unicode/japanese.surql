/**
[test]
reason = "Test ORDER BY with Japanese characters (hiragana, katakana, kanji)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 'あ' }, { value: 'い' }, { value: 'う' }, { value: 'え' }, { value: 'お' }, { value: 'ア' }, { value: 'イ' }, { value: 'ウ' }, { value: '日' }, { value: '本' }, { value: '語' }]"

[[test.results]]
value = "[{ value: '語' }, { value: '本' }, { value: '日' }, { value: 'ウ' }, { value: 'イ' }, { value: 'ア' }, { value: 'お' }, { value: 'え' }, { value: 'う' }, { value: 'い' }, { value: 'あ' }]"

[[test.results]]
value = "[{ value: 'あ' }, { value: 'ア' }, { value: '本' }, { value: 'え' }, { value: 'い' }, { value: 'イ' }, { value: 'お' }, { value: '日' }, { value: 'う' }, { value: 'ウ' }, { value: '語' }]"

[[test.results]]
value = "[{ value: '語' }, { value: 'ウ' }, { value: 'う' }, { value: '日' }, { value: 'お' }, { value: 'イ' }, { value: 'い' }, { value: 'え' }, { value: '本' }, { value: 'ア' }, { value: 'あ' }]"

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
SELECT `value` FROM item ORDER BY value ASC;

-- Test without COLLATE DESC
SELECT `value` FROM item ORDER BY value DESC;

-- Test with COLLATE ASC: Unicode-aware ordering
SELECT `value` FROM item ORDER BY value COLLATE ASC;

-- Test with COLLATE DESC
SELECT `value` FROM item ORDER BY value COLLATE DESC;
