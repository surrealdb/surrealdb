/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, settings: { nested: { object: { thing: 'test' } } } }]"

[[test.results]]
value = "[{ id: person:test, settings: { nested: { object: { thing: 'test' } } } }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD settings on person FLEXIBLE TYPE object;
UPSERT person:test CONTENT {
	settings: {
		nested: {
			object: {
				thing: 'test'
			}
		}
	}
};
SELECT * FROM person;
