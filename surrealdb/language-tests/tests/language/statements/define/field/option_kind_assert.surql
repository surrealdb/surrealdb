/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
value = "[{ id: person:mark, name: 'mark' }]"

[[test.results]]
error = "Found 'bob' for field `name`, with record `person:bob`, but field must conform to: string::len($value) > 3"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON TABLE person TYPE option<string> ASSERT string::len($value) > 3;
CREATE person:test;
CREATE person:mark SET name = 'mark';
CREATE person:bob SET name = 'bob';
