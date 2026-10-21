/**
[env]
namespace = true
database = true

[test]
reason = "An outer WHERE over a subquery is judged on the rows the subquery returns. A LIMIT or START cuts the rows before the filter, so a predicate placed in the subquery's loop body must not be counted before the paging: the outer filter on id or on an indexed or non-indexed field returns only the rows inside the window, and an expression that can raise is not evaluated on a row outside it."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:3 }]"

[[test.results]]
value = "[{ id: person:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:5, name: 'p5' }]"

[[test.results]]
value = "[1f]"

[[test.results]]
value = "[1f, 3f, 5f]"

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
value = "[{ id: person:2 }]"

[[test.results]]
value = "[{ id: person:2 }, { id: person:3 }]"

[[test.results]]
value = "[{ id: person:3 }]"

[[test.results]]
value = "[{ id: person:3 }]"

[[test.results]]
value = "[{ id: person:2 }]"

[[test.results]]
value = "[{ owner: 'x' }]"

[[test.results]]
value = "[]"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE INDEX ip ON person FIELDS age;
	CREATE person:1 SET name = 'p1', age = 10, tags = ['a'], score = 1.5, owner = 'x';
	CREATE person:2 SET name = 'p2', age = 20, tags = ['b'], score = 2.5, owner = 'y';
	CREATE person:3 SET name = 'p3', age = 30, tags = ['a'], score = 3.5, owner = 'x';
	CREATE person:4 SET name = 'p4', age = 40, tags = ['c'], score = 4.5, owner = 'y';
	CREATE person:5 SET name = 'p5', age = 50, tags = ['a', 'b'], score = 5.5, owner = 'x';
	RETURN 'OK';
};

-- The outer WHERE on id, with the subquery ordered by id or by an indexed field, or unordered.
SELECT id FROM (SELECT * FROM person ORDER BY id LIMIT 2) WHERE id = person:3;
SELECT id FROM (SELECT * FROM person LIMIT 2) WHERE id = person:3;
SELECT id FROM (SELECT * FROM person START 2) WHERE id = person:3;
SELECT id FROM (SELECT * FROM person LIMIT 2 START 1) WHERE id = person:3;
SELECT id FROM (SELECT * FROM person ORDER BY age LIMIT 2) WHERE id = person:3;
-- The outer WHERE on a non-indexed field, and on the indexed field.
SELECT id FROM (SELECT * FROM person LIMIT 2) WHERE name = 'p3';
SELECT id FROM (SELECT * FROM person LIMIT 2) WHERE age = 30;
SELECT id FROM (SELECT * FROM person ORDER BY age LIMIT 2) WHERE age = 30;
SELECT id FROM (SELECT * FROM person ORDER BY age LIMIT 2) WHERE name = 'p3';
-- A projection that renames the filtered field.
SELECT id, name FROM (SELECT id, name FROM person ORDER BY age LIMIT 5) WHERE name = 'p5';
-- An outer projection that can raise must not see a row outside the window.
SELECT VALUE math::floor(score) FROM (SELECT id, tags, score FROM person ORDER BY age LIMIT 2) WHERE 'a' INSIDE tags;
SELECT VALUE math::floor(score) FROM (SELECT id, tags, score FROM person ORDER BY age LIMIT 5) WHERE 'a' INSIDE tags;
-- An outer IN over the id.
SELECT id FROM (SELECT * FROM person LIMIT 2) WHERE id IN [person:3, person:1];
SELECT id FROM (SELECT * FROM person LIMIT 2) WHERE id = person:2;
SELECT id FROM (SELECT * FROM person LIMIT 3) WHERE age >= 20;
-- Without a limit the filter still moves and returns the same rows.
SELECT id FROM (SELECT * FROM person ORDER BY age) WHERE age = 30;
SELECT id FROM (SELECT * FROM person LIMIT 5) WHERE id = person:3;
SELECT id FROM (SELECT * FROM person LIMIT 2) WHERE owner = 'y';
-- A grouping beneath the filter.
SELECT owner FROM (SELECT owner, count() AS c FROM person GROUP BY owner) WHERE owner = 'x';
SELECT name, c FROM (SELECT name, count() AS c FROM person GROUP BY name ORDER BY name LIMIT 2) WHERE name = 'p3';
