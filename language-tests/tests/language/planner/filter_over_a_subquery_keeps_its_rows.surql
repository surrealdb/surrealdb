/**
[env]

[test]
reason = "A filter over a subquery judged beneath the subquery's projection returns the rows the filter judged above it returned: a conjunct that renames, ORs, calls a function or can raise, and a subquery that splits, groups, limits or orders its rows"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, b: 1, c: 3, id: t:2 }]"

[[test.results]]
value = "[{ a: 1, b: 1, id: t:3 }]"

[[test.results]]
value = "[{ b: 2, id: t:1, z: 1 }, { b: 1, id: t:3, z: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: t:1, z: 1 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 2, id: t:2 }, { a: 1, id: t:3 }]"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `2`"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, b: 1, id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, z: 1 }, { id: t:3, z: 1 }]"

[[test.results]]
value = "[{ a: 1, c: 3, id: t:1 }, { a: 1, id: t:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ y: NONE, z: 1 }, { y: NONE, z: 1 }]"

[[test.results]]
value = "[{ z: 2 }, { z: 2 }]"

[[test.results]]
value = "[{ id: t:1, z: 1 }, { id: t:3, z: 1 }]"

[[test.results]]
value = "[{ id: t:3, z: 1 }]"

[[test.results]]
value = "[{ z: 1 }, { z: 1 }]"

[[test.results]]
value = "[{ a: 1, n: 2 }]"

[[test.results]]
value = "[{ id: t:3, z: 1 }]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;
DEFINE INDEX ib ON t FIELDS b;
CREATE t:1 SET a = 1, b = 2, c = 3;
CREATE t:2 SET a = 2, b = 1, c = 3;
CREATE t:3 SET a = 1, b = 1;
SELECT * FROM (SELECT a AS z, b, id FROM t) WHERE z = 1 ORDER BY id;
SELECT * FROM (SELECT *, a AS z FROM t) WHERE z = 1 AND c = 3 ORDER BY id;
SELECT * FROM (SELECT a, id FROM t) WHERE a = 1 OR a = 2 ORDER BY id;
SELECT * FROM (SELECT * FROM t) WHERE string::len(b) > 1;
SELECT * FROM (SELECT * FROM t) WHERE <int> b = 2;
SELECT * FROM (SELECT b AS a, a AS b, id FROM t) WHERE a = 2 AND b = 1 ORDER BY id;
SELECT * FROM (SELECT a AS z, id FROM t) WHERE z = 1 AND z = $parent.x ORDER BY id;
SELECT * FROM (SELECT a AS z, id FROM t) WHERE $this.z = 1 ORDER BY id;
SELECT * FROM (SELECT * OMIT b FROM t) WHERE a = 1 ORDER BY id;
SELECT * FROM (SELECT * OMIT b FROM t) WHERE b = 1;
SELECT * FROM (SELECT a AS z, a.b AS y FROM t) WHERE z = 1;
SELECT * FROM (SELECT a + 1 AS z FROM t) WHERE z = 2;
SELECT * FROM (SELECT a AS z, id FROM t LIMIT 2) WHERE z = 1 ORDER BY id;
SELECT * FROM (SELECT a AS z, id FROM t ORDER BY b, id LIMIT 2) WHERE z = 1 ORDER BY id;
SELECT * FROM (SELECT a AS z FROM t SPLIT a) WHERE z = 1;
SELECT * FROM (SELECT a, count() AS n FROM t GROUP BY a) WHERE a = 1;
SELECT * FROM (SELECT a AS z, id FROM t) WHERE z = 1 AND id = t:3;
