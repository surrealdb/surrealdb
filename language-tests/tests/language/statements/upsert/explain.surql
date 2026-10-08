/**
[env]

[test]
reason = "The rows of an indexed `UPSERT` are the same whatever plan runs it: the statement writes through the index and the selects around it read the result. `explain_streaming.surql` pins the plan."

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ id: person:test, name: 'bar' }]"

[[test.results]]
value = "[{ id: person:test, name: 'bar' }]"

*/
CREATE person:test SET name = 'foo';
DEFINE INDEX idx ON person FIELDS name;
SELECT * FROM person;
SELECT * FROM person;
SELECT * FROM person;
UPSERT person SET name = 'bar' WHERE name = 'foo';
SELECT * FROM person;
