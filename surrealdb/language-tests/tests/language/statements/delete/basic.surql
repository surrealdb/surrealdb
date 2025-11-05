/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

CREATE person:test SET name = 'Tester';
DELETE person:test;
SELECT * FROM person;
