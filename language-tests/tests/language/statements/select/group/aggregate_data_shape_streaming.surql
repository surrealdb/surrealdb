/**
[env]
namespace = true
database = true

[test]
reason = "An aggregate's argument or key whose evaluation fails only about the shape of the data is NONE, which the aggregate folds in as a value it leaves out (a sum skips it); an error of any other kind, such as the remainder of NONE, still fails the statement. A statement that groups rows of mixed shape would otherwise fail on its first odd row."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ city: NONE, s: 0 }, { city: 'x', s: 94 }, { city: 'y', s: 0 }]"

[[test.results]]
value = "[{ hi: 29, m: 24.5f }]"

[[test.results]]
error = "Cannot perform remainder with 'none' and 'int'"

[[test.results]]
value = "[{ t: 77 }]"

[[test.results]]
value = "[{ c: 0 }]"

[[test.results]]
value = "[{ k: 1, s: 5 }, { k: 2, s: 0 }]"

*/

{
	CREATE person:1 SET name = 'ann', age = 30, city = 'x';
	CREATE person:2 SET name = 'bob', age = 17, city = 'x';
	CREATE person:3 SET name = 'cat', city = 'y';
	CREATE person:4 SET name = 'dan';
	RETURN 'OK';
};

-- an argument that is NONE for a row
SELECT city, math::sum(age * 2) AS s FROM person GROUP BY city ORDER BY city;
SELECT math::mean(age + 1) AS m, math::max(age - 1) AS hi FROM person GROUP ALL;

-- a key whose evaluation fails for another reason fails the statement
SELECT age % 2 AS parity, count() AS c FROM person GROUP BY parity ORDER BY parity;

-- an answer over aggregates that are NONE
SELECT math::sum(age) + math::max(age) AS t FROM person GROUP ALL;

-- an argument whose operands do not add up
SELECT count(age + 'x') AS c FROM person GROUP ALL;
SELECT k, math::sum(a + 1) AS s FROM [{ k: 1, a: 1 }, { k: 1, a: 2 }, { k: 2, a: 'x' }] GROUP BY k ORDER BY k;
