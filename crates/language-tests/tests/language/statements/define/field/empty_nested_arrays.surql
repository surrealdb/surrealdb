/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, settings: { nested: [1, 2, 3, 4, 5] } }]"

[[test.results]]
value = "[{ id: person:test, settings: { nested: [1, 2, 3, 4, 5] } }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD settings ON person TYPE object FLEXIBLE;
UPSERT person:test CONTENT {
	settings: {
		nested: [
			1,
			2,
			3,
			4,
			5
		]
	}
};
SELECT * FROM person;
