/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ details: '', error: 'Found number keys with different types in the same position within a record-id key which will have a different order in 3.0', kind: 'number key ordering', origin: '/ns/test/db/test/table/foo/record/{ a: [{ b: [false, 0f], c: true }] }', severity: 'can_break' }]"

*/

{
CREATE foo:{a: [{b: [false,1], c: true}]};
CREATE foo:{a: [{b: [false,0f], c: true}]};
};

migration::diagnose()
