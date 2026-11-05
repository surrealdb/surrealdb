/**
[test]
reason = "A SELECT in FROM position is planned from its own logical plan: a predicate pushed into it returns the same rows, and its own FOR UPDATE clause rules are enforced at any depth"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ age: 40, id: person:2, name: 'b' }]"

[[test.results]]
value = "[{ who: 'b' }]"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'a' }]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with LIMIT over multiple targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with VERSION"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

*/

{
	CREATE person:1 SET name = 'a', age = 30;
	CREATE person:2 SET name = 'b', age = 40;
	CREATE person:3 SET name = 'c', age = 10;
	RETURN 'OK';
};

-- A predicate over a `SELECT *` subquery merges with the subquery's own.
SELECT * FROM (SELECT * FROM person WHERE age > 20) WHERE name = 'b';

-- A projecting subquery keeps the outer predicate outside it.
SELECT * FROM (SELECT name AS who FROM person WHERE age > 20) WHERE who = 'b';

-- A locking read inside a subquery source is allowed.
SELECT * FROM (SELECT * FROM person:1 FOR UPDATE);

-- The FOR UPDATE clause rules apply to the subquery's own clauses.
SELECT * FROM (SELECT count() FROM person:1 GROUP ALL FOR UPDATE);
SELECT * FROM (SELECT * FROM person:1 SPLIT tags FOR UPDATE);
SELECT * FROM (SELECT * FROM person:1, person:2 LIMIT 1 FOR UPDATE);
SELECT * FROM (SELECT * FROM person:1 VERSION d'2024-01-01T00:00:00Z' FOR UPDATE);
SELECT * FROM (SELECT * FROM person FOR UPDATE);

-- Lowering distributes FOR UPDATE onto stored sources, so a statement whose
-- only source is itself a subquery leaves no trace of the clause in the plan.
-- The rules still have to reach it, at any nesting depth.
SELECT * FROM (SELECT * FROM (SELECT * FROM person:1) FOR UPDATE);
SELECT * FROM (SELECT count() FROM (SELECT * FROM person) GROUP ALL FOR UPDATE);
