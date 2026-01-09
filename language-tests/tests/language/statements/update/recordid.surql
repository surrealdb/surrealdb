/**
[test]

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
error = "Found person:other for the `id` field, but a specific record has been specified"

[[test.results]]
value = "[{ id: person:test }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }, { id: person:test }]"

*/

CREATE person:test;
SELECT * FROM person;

UPDATE person:test SET id = person:other;
SELECT * FROM person;

CREATE person:bob CONTENT {
    name: "Bob"
};
SELECT * FROM person;
