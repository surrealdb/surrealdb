/**
[test]
reason = "Subqueries in SELECT and RETURN return correct array/object results"

[[test.results]]
value = "[{ age: 21, id: person:test, name: 'Tobie' }]"

[[test.results]]
value = "[{ adult: true }]"

[[test.results]]
value = "[{ adult: true }]"

[[test.results]]
value = "[{ adult: true }]"

[[test.results]]
value = "[{ adult: true }]"

[[test.results]]
value = "[{ adult: true }]"

[[test.results]]
value = "[{ adult: true }]"
*/

CREATE person:test SET name = 'Tobie', age = 21;
-- Select all records, returning an array
SELECT age >= 18 as adult FROM person;
-- Select a specific record, still returning an array
SELECT age >= 18 as adult FROM person:test;
-- Select all records in a subquery, returning an array
RETURN (SELECT age >= 18 AS adult FROM person);
-- Select a specific record in a subquery, returning an object
RETURN (SELECT age >= 18 AS adult FROM person:test);
-- Using an outer SELECT, select all records in a subquery, returning an array
SELECT * FROM (SELECT age >= 18 AS adult FROM person) WHERE adult = true;
-- Using an outer SELECT, select a specific record in a subquery, returning an array
SELECT * FROM (SELECT age >= 18 AS adult FROM person:test) WHERE adult = true;
