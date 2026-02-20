/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found future which can be stored in the datastore', kind: 'stored closure', location: { column: 34, kind: 'error', length: 11, line: 1, source: 'DEFINE FIELD field ON t VALUE {a:||{(1 + 1)}', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 't', 'field', 'field'], severity: 'will_break' }]"

*/
DEFINE FIELD field ON t VALUE {
	a: || { 1 + 1 },
};

migration::diagnose();
