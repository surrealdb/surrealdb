/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, name: 'Tobie' }]"

[[test.results]]
error = "Couldn't coerce value for field `name` of `person:test`: Expected `none | string` but found `NULL`"

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:test, name: NULL }]"

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:test, name: NULL }]"

[[test.results]]
error = "Couldn't coerce value for field `name` of `person:test`: Expected `string | null` but found `NONE`"

*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON TABLE person TYPE option<string>;
UPSERT person:test SET name = 'Tobie';
UPSERT person:test SET name = NULL;
UPSERT person:test SET name = NONE;
--
REMOVE TABLE person;
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON TABLE person TYPE option<string | null>;
UPSERT person:test SET name = 'Tobie';
UPSERT person:test SET name = NULL;
UPSERT person:test SET name = NONE;
--
REMOVE TABLE person;
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON TABLE person TYPE string | null;
UPSERT person:test SET name = 'Tobie';
UPSERT person:test SET name = NULL;
UPSERT person:test SET name = NONE;
