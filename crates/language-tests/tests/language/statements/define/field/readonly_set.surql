/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ birthdate: d'2023-12-13T21:27:55.632Z', id: person:test }]"

[[test.results]]
value = "[{ birthdate: d'2023-12-13T21:27:55.632Z', id: person:test }]"

[[test.results]]
error = "Found changed value for field `birthdate`, with record `person:test`, but field is readonly"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD birthdate ON person TYPE datetime READONLY;
CREATE person:test SET birthdate = d'2023-12-13T21:27:55.632Z';
UPSERT person:test SET birthdate = d'2023-12-13T21:27:55.632Z';
UPSERT person:test SET birthdate = d'2024-12-13T21:27:55.632Z';
