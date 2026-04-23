/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `age` of `person:test`: Expected `int` but found `NONE`"

[[test.results]]
error = "Couldn't coerce value for field `enabled` of `person:test`: Expected `bool | int` but found `NONE`"

[[test.results]]
error = "Couldn't coerce value for field `name` of `person:test`: Expected `string` but found `NONE`"

[[test.results]]
value = "[{ age: 18, enabled: true, id: person:test, name: 'Tobie Morgan Hitchcock', scores: {1f, 2f, 3f, 4f, 5f} }]"

*/
DEFINE FIELD age ON person TYPE int;
DEFINE FIELD enabled ON person TYPE bool | int;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD scores ON person TYPE set<float, 5>;
UPSERT person:test SET age = NONE, enabled = NONE, name = NONE, scores = {1,1,2,2,3,3,4,4,5,5};
UPSERT person:test SET age = 18, enabled = NONE, name = NONE, scores = {1,1,2,2,3,3,4,4,5,5};
UPSERT person:test SET age = 18, enabled = true, name = NONE, scores = {1,1,2,2,3,3,4,4,5,5};
UPSERT person:test SET age = 18, enabled = true, name = 'Tobie Morgan Hitchcock', scores = {1,1,2,2,3,3,4,4,5,5};
