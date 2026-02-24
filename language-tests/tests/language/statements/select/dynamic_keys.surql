/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test dynamic array and object key access in SELECT and UPSERT"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: documentation:test, languages: { de: 'das ist Englisch', en: 'this is english', es: 'esto es español' }, primarylang: 'en', tags: [{ type: 'library', value: 'client-side' }, { type: 'library', value: 'server-side' }, { type: 'environment', value: 'frontend' }] }]"

[[test.results]]
value = "[{ tags: { value: 'client-side' } }]"

[[test.results]]
value = "[{ content: 'this is english' }]"

[[test.results]]
value = "[{ id: documentation:test, languages: { de: 'das ist Englisch', en: 'my primary text', es: 'esto es español' }, primarylang: 'en', tags: [{ type: 'library', value: 'client-side' }, { type: 'library', value: 'server-side' }, { type: 'environment', value: 'frontend' }] }]"

[[test.results]]
value = "[{ content: 'my primary text' }]"

[[test.results]]
value = "[{ id: documentation:test, languages: { de: 'das ist Englisch', en: 'my secondary text', es: 'esto es español' }, primarylang: 'en', tags: [{ type: 'library', value: 'client-side' }, { type: 'library', value: 'server-side' }, { type: 'environment', value: 'frontend' }] }]"

[[test.results]]
value = "[{ content: 'my secondary text' }]"
*/

LET $lang = 'en';
UPSERT documentation:test CONTENT {
	primarylang: 'en',
	languages: {
		'en': 'this is english',
		'es': 'esto es español',
		'de': 'das ist Englisch',
	},
	tags: [
		{ type: 'library', value: 'client-side' },
		{ type: 'library', value: 'server-side' },
		{ type: 'environment', value: 'frontend' },
	]
};
-- An array filter, followed by an array index operation
SELECT tags[WHERE type = 'library'][0].value FROM documentation:test;
-- Selecting an object value using a string as a key
SELECT languages['en'] AS content FROM documentation:test;
-- Updating an object value using a string as a key
UPSERT documentation:test SET languages['en'] = 'my primary text';
-- Selecting an object value using a parameter as a key
SELECT languages[$lang] AS content FROM documentation:test;
-- Updating an object value using a parameter as a key
UPSERT documentation:test SET languages[$lang] = 'my secondary text';
-- Selecting using the value of another document field as a key
SELECT languages[primarylang] AS content FROM documentation;
