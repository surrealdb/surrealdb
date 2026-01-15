/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found number keys with different types in the same position within a record-id key which will have a different order in 3.0', kind: 'number key ordering', origin: '/ns/test/db/test/table/foo/record/{ a: 1f }', severity: 'can_break' }]"

*/

{
	CREATE foo:{a: 1};
	CREATE foo:{a: 1f};

	CREATE bar:{a: 1};
	CREATE bar:{b: 1f};
};

migration::diagnose()
