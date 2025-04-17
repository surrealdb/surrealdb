/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 18, id: person:test }]"

[[test.results]]
value = "[{ age: 18, id: person:test, metadata: true }]"

[[test.results]]
value = "[{ age: 18, id: person:test, metadata: false }]"

*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD age ON person TYPE number;
DEFINE FIELD metadata ON person FLEXIBLE TYPE any;

CREATE person:test CONTENT { age: 18 };
UPSERT person:test SET metadata = true, something = 'some';
UPDATE person:test MERGE { metadata: false, something: 'thing' };
