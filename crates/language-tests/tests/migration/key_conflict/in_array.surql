/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found number keys with different types in the same position within a record-id key which will have a different order in 3.0', kind: 'number key ordering', origin: ['ns', 'test', 'db', 'test', 'table', 'bar', 'record', [1dec]], severity: 'can_break' }, { details: '', error: 'Found number keys with different types in the same position within a record-id key which will have a different order in 3.0', kind: 'number key ordering', origin: ['ns', 'test', 'db', 'test', 'table', 'baz', 'record', [1dec]], severity: 'can_break' }, { details: '', error: 'Found number keys with different types in the same position within a record-id key which will have a different order in 3.0', kind: 'number key ordering', origin: ['ns', 'test', 'db', 'test', 'table', 'foo', 'record', [1f]], severity: 'can_break' }]"

*/

{
	CREATE foo:[1];
	CREATE foo:[1f];

	CREATE bar:[1];
	CREATE bar:[1dec];

	CREATE baz:[1f];
	CREATE baz:[1dec];
};

migration::diagnose()
