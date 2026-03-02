/**
[test]
reason = "Test dynamic array and object key access in SELECT and UPSERT (new executor)"
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
