/**
[env]
namespace = true
database = true

[test]
reason = "A group's keys, aggregate arguments, arguments after the first, answers over several aggregates and constant columns are evaluated as GROUP BY and GROUP ALL define them: a constant column is evaluated once for its group, and an answer is computed over the group's aggregates and keys. Integers stay integers through a sum (an average of ints is an int)."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ city: NONE, s: 90 }, { city: 'x', s: 94 }, { city: 'y', s: 104 }]"

[[test.results]]
value = "[{ hi: 44, m: 29.8f, sd: 10.61602562167217f }]"

[[test.results]]
value = "[{ adults: 4 }]"

[[test.results]]
value = "[{ c: 3, parity: 0 }, { c: 2, parity: 1 }]"

[[test.results]]
value = "[{ c: 5, l: 3 }]"

[[test.results]]
value = "[{ avg: 45, city: NONE }, { avg: 23, city: 'x' }, { avg: 26, city: 'y' }]"

[[test.results]]
value = "[{ age: 17, t: 34 }, { age: 22, t: 44 }, { age: 30, t: 90 }, { age: 45, t: 90 }]"

[[test.results]]
value = "[{ t: 189 }]"

[[test.results]]
value = "[{ j: 'ann-bob-cat-dan-eve' }]"

[[test.results]]
value = "[{ city: NONE, j: 'eve' }, { city: 'x', j: 'ann<x>bob' }, { city: 'y', j: 'cat<y>dan' }]"

[[test.results]]
value = "[{ c: 5, label: 'k' }]"

[[test.results]]
value = "[{ c: 0, k: 7 }]"

[[test.results]]
value = "[{ c: 1, city: NONE, seven: 7, z: NONE }, { c: 2, city: 'x', seven: 7, z: NONE }, { c: 2, city: 'y', seven: 7, z: NONE }]"

[[test.results]]
value = "[{ k: 1, s: 5 }, { k: 2, s: 4 }]"

[[test.results]]
value = "[{ s: 194, tags: NONE }, { s: 60, tags: 'a' }, { s: 94, tags: 'b' }]"

*/

{
	CREATE person:1 SET name = 'ann', age = 30, city = 'x', tags = ['a', 'b'];
	CREATE person:2 SET name = 'bob', age = 17, city = 'x', tags = ['b'];
	CREATE person:3 SET name = 'cat', age = 30, city = 'y';
	CREATE person:4 SET name = 'dan', age = 22, city = 'y';
	CREATE person:5 SET name = 'eve', age = 45;
	RETURN 'OK';
};

-- computed arguments
SELECT city, math::sum(age * 2) AS s FROM person GROUP BY city ORDER BY city;
SELECT math::mean(age + 1) AS m, math::max(age - 1) AS hi, math::stddev(age) AS sd FROM person GROUP ALL;
SELECT count(age > 20) AS adults FROM person GROUP ALL;

-- computed keys
SELECT age % 2 AS parity, count() AS c FROM person GROUP BY parity ORDER BY parity;
SELECT string::len(name) AS l, count() AS c FROM person GROUP BY l;

-- an answer over several aggregates and keys
SELECT city, math::sum(age) / count() AS avg FROM person GROUP BY city ORDER BY city;
SELECT age, math::sum(age) + age AS t FROM person GROUP BY age ORDER BY age;
SELECT math::sum(age) + math::max(age) AS t FROM person GROUP ALL;

-- an argument after the first
SELECT array::join(name, '-') AS j FROM person GROUP ALL;
SELECT city, array::join(name, string::concat('<', city, '>')) AS j FROM person GROUP BY city ORDER BY city;

-- constant columns
SELECT 'k' AS label, count() AS c FROM person GROUP ALL;
SELECT math::abs(-7) AS k, count() AS c FROM person WHERE age > 1000 GROUP ALL;
SELECT city, $missing AS z, 7 AS seven, count() AS c FROM person GROUP BY city ORDER BY city;

-- rows that are not records
SELECT k, math::sum(a + 1) AS s FROM [{ k: 1, a: 1 }, { k: 1, a: 2 }, { k: 2, a: 3 }] GROUP BY k ORDER BY k;

-- SPLIT before GROUP
SELECT tags, math::sum(age * 2) AS s FROM (SELECT tags, age FROM person SPLIT tags) GROUP BY tags ORDER BY tags;
