/**
[env]
namespace = true
database = true
imports = ["pgsql/_intersect_except_tables.surql"]

[test]
reason = "INTERSECT keeps the distinct rows of the first query that the second query also has, and EXCEPT the ones it does not have, over one or several columns, with NULL equal to NULL, under the names of the first query, with an ORDER BY, LIMIT and OFFSET that apply to the answer; the expected rows are those of PostgreSQL 17"

[[test.results]]
value = "[{ v: 1 }, { v: 3 }, { v: NULL }]"

[[test.results]]
value = "[{ v: 2 }]"

[[test.results]]
value = "[{ v: 4 }]"

[[test.results]]
value = "[{ v: 1, w: 'x' }, { v: 3, w: 'z' }, { v: NULL, w: 'n' }]"

[[test.results]]
value = "[{ v: 2, w: 'y' }]"

[[test.results]]
value = "[{ a: 1 }, { a: 3 }, { a: NULL }]"

[[test.results]]
value = "[{ v: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ v: 1 }, { v: 2 }, { v: 3 }, { v: NULL }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 3 }]"

[[test.results]]
value = "[{ n: 3 }]"

[[test.results]]
value = "[{ x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ v: 1 }, { v: 3 }]"
*/

SELECT v FROM ie_left INTERSECT SELECT v FROM ie_right ORDER BY v;
SELECT v FROM ie_left EXCEPT SELECT v FROM ie_right ORDER BY v;
SELECT v FROM ie_right EXCEPT SELECT v FROM ie_left ORDER BY v;
SELECT v, w FROM ie_left INTERSECT SELECT v, w FROM ie_right ORDER BY v;
SELECT v, w FROM ie_left EXCEPT SELECT v, w FROM ie_right ORDER BY v;
SELECT v AS a FROM ie_left INTERSECT SELECT v AS b FROM ie_right ORDER BY a;
SELECT v FROM ie_left INTERSECT SELECT v FROM ie_right ORDER BY v LIMIT 1 OFFSET 1;
SELECT v FROM ie_left INTERSECT SELECT v FROM ie_right WHERE v > 100;
SELECT v FROM ie_left EXCEPT SELECT v FROM ie_right WHERE v > 100 ORDER BY v;
SELECT v FROM ie_left WHERE v > 100 EXCEPT SELECT v FROM ie_right;
SELECT count(*) AS n FROM (SELECT v FROM ie_left INTERSECT SELECT v FROM ie_right) s;
SELECT v + 1 AS n FROM ie_left EXCEPT SELECT v + 1 AS n FROM ie_right ORDER BY n;
SELECT 1 AS x INTERSECT SELECT 1 AS x;
SELECT 1 AS x EXCEPT SELECT 1 AS x;
SELECT a.v FROM ie_left a JOIN ie_right b ON a.id = b.id INTERSECT SELECT v FROM ie_right ORDER BY v;
