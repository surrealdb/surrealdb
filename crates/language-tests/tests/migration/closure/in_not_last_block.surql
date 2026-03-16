/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found future which can be stored in the datastore', kind: 'stored closure', location: { column: 36, kind: 'error', length: 11, line: 1, source: 'DEFINE FIELD field ON t2 VALUE {1; ||{(1 + 1)}', truncation: 'none' }, origin: ['ns', 'test', 'db', 'test', 'table', 't2', 'field', 'field'], severity: 'will_break' }]"

*/
DEFINE FIELD field ON t VALUE {
	|| { 1 + 1 };
	1
};
DEFINE FIELD field ON t2 VALUE {
	1;
	|| { 1 + 1 }
};
migration::diagnose();
