/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, settings: {  } }]"

[[test.results]]
value = "[{ id: person:test, settings: {  } }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD settings on person TYPE object;
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
