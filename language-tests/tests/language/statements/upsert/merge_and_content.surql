/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Jaime' }]"

[[test.results]]
error = "Cannot use 'some content' in a CONTENT clause"

[[test.results]]
error = "Cannot use 'some content' in a CONTENT clause"

[[test.results]]
value = "[{ age: 50, id: person:test, name: 'Jaime' }]"

[[test.results]]
error = "Cannot use 'some content' in a MERGE clause"

*/
CREATE person:test CONTENT { name: 'Tobie' };
UPSERT person:test CONTENT { name: 'Jaime' };
UPSERT person:test CONTENT 'some content';
UPSERT person:test REPLACE 'some content';
UPSERT person:test MERGE { age: 50 };
UPSERT person:test MERGE 'some content';
