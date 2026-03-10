/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'settings.nested', but no such field exists for table 'person'"

[[test.results]]
value = "[{ id: person:test, settings: {} }]"

[[test.results]]
value = "[{ id: person:test, settings: {} }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD settings ON person TYPE object;
UPSERT person:test CONTENT {
	settings: {
		nested: {
			object: {
				thing: 'test'
			}
		}
	}
};
UPSERT person:test CONTENT {
	settings: {}
};
SELECT * FROM person;
