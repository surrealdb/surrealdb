/**
[env]

[test]
reason = "The rows of an indexed `DELETE` are the same whatever plan runs it: the statement writes through the index and the selects around it read the result. `explain_streaming.surql` pins the plan."

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/
CREATE person:test SET name = 'Tester';
DEFINE INDEX idx ON person FIELDS name;
SELECT * FROM person;
SELECT * FROM person;
SELECT * FROM person;
DELETE person WHERE name = 'Tester';
SELECT * FROM person;
