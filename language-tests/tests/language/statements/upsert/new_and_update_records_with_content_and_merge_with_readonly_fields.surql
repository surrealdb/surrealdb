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
value = "[{ age: 1, created: d'2024-01-01T00:00:00Z', data: { other: false, some: true }, id: person:test }]"

[[test.results]]
value = "[{ age: 2, created: d'2024-01-01T00:00:00Z', data: { nothing: true }, id: person:test }]"

[[test.results]]
value = "[{ age: 3, created: d'2024-01-01T00:00:00Z', data: { nothing: true }, id: person:test }]"

[[test.results]]
value = "[{ age: 4, created: d'2024-01-01T00:00:00Z', data: { nothing: false }, id: person:test }]"

[[test.results]]
error = "Found changed value for field `created`, with record `person:test`, but field is readonly"

*/
-- Setup the schemaful table
DEFINE TABLE person SCHEMALESS;
DEFINE FIELD created ON person TYPE datetime READONLY DEFAULT d'2024-01-01';
DEFINE FIELD age ON person TYPE number;
DEFINE FIELD data ON person TYPE object;
-- This record will be created successfully
UPSERT person:test CONTENT { age: 1, data: { some: true, other: false } };
-- This record will be updated successfully, with the readonly field untouched
UPSERT person:test CONTENT { age: 2, data: { nothing: true } };
-- This record will be updated successfully, with the readonly and flexible fields untouched
UPSERT person:test MERGE { age: 3 };
-- This record will be updated successfully, with the readonly and flexible fields untouched
UPSERT person:test SET age = 4, data.nothing = false;
-- This will return an error, as the readonly field is modified
UPSERT person:test REPLACE { age: 5, data: { other: true } };
