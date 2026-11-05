/**
[env]
namespace = true
database = true
imports = ["pgsql/_union_tables.surql"]

[test]
reason = "A derived table is read under the column names its alias lists, and a list of VALUES is a derived table: (VALUES (1, 'a'), (2, 'b')) AS v(n, l). The first columns are renamed in order, and a column the list does not reach keeps its name, including the name a CTE's own column list gave it"

[[test.results]]
value = "[{ n: 1 }, { n: 2 }]"

[[test.results]]
value = "[{ l: 'a', n: 1 }, { l: 'b', n: 2 }, { l: 'c', n: 3 }]"

[[test.results]]
value = "[{ column2: 'a', n: 1 }, { column2: 'b', n: 2 }]"

[[test.results]]
value = "[{ column1: 1, column2: 'a' }, { column1: 2, column2: 'b' }]"

[[test.results]]
value = "[{ who: 'ada' }, { who: 'bob' }, { who: 'bob' }, { who: 'cy' }]"

[[test.results]]
value = "[{ total: 3 }]"

[[test.results]]
value = "[{ l: 'x' }, { l: 'y' }]"

[[test.results]]
value = "[{ t: 1 }]"

[[test.results]]
value = "[{ l: NULL, n: 1 }, { l: 'c', n: 3 }, { l: 'b', n: NULL }]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "[{ n: 1 }, { n: 2 }]"

[[test.results]]
value = "[{ c: 2, s: 4 }]"

[[test.results]]
value = "[{ who: 'cy' }, { who: 'bob' }]"

[[test.results]]
value = "[{ b: 'ada', z: 1 }, { b: 'bob', z: 2 }]"
*/

SELECT v.num AS n FROM (SELECT k FROM un_a) AS v(num) ORDER BY n;
SELECT n, l FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(n, l) ORDER BY n;
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(n) ORDER BY n;
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v ORDER BY column1;
SELECT who FROM (SELECT name FROM un_a UNION ALL SELECT label FROM un_b) AS u(who) ORDER BY who;
SELECT sum(n) AS total FROM (VALUES (1), (2)) AS v(n);
SELECT l FROM (VALUES (1), (2)) AS a(n) JOIN (VALUES (1, 'x'), (2, 'y')) AS b(k, l) ON a.n = b.k ORDER BY l;
SELECT v."Total" AS t FROM (VALUES (1)) AS v("Total");
SELECT n, l FROM (VALUES (1, NULL), (NULL, 'b'), (3, 'c')) AS v(n, l) ORDER BY n;
SELECT n FROM (SELECT k FROM un_a) AS v(n) WHERE n = 2;
WITH c AS (SELECT k FROM un_a) SELECT n FROM c AS v(n) ORDER BY n;
SELECT count(n) AS c, sum(n) AS s FROM (VALUES (1), (NULL), (3)) AS v(n);
SELECT who FROM (SELECT name FROM un_a UNION ALL SELECT label FROM un_b ORDER BY name DESC LIMIT 2) AS u(who) ORDER BY who DESC;
WITH c(a, b) AS (SELECT k AS x, name AS y FROM un_a) SELECT z, b FROM c AS v(z) ORDER BY z;
