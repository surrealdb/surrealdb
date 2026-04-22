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
UPDATE person:test CONTENT { name: 'Jaime' };
UPDATE person:test CONTENT 'some content';
UPDATE person:test REPLACE 'some content';
UPDATE person:test MERGE { age: 50 };
UPDATE person:test MERGE 'some content';
