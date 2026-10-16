/**
[env]

[test]
reason = "A filter over a subquery that names its fields admits the rows it admits when it is judged above the projection: a name the projection lands reads the field it landed, a name it passes through reads the same field, and a name it drops reads nothing"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 10, c: 'x', id: t:1 }]"

[[test.results]]
value = "[{ a: 2, b: 20, c: 'y', id: t:2 }]"

[[test.results]]
value = "[{ a: 1, b: 30, id: t:3 }]"

[[test.results]]
value = "[{ b: 10, z: 1 }, { b: 30, z: 1 }]"

[[test.results]]
value = "[{ a: 1, b: 30 }]"

[[test.results]]
value = "[{ a: 1, b: 10, c: 'x', id: t:1, z: 1 }]"

[[test.results]]
value = "[{ b: 10, z: true }, { b: 30, z: true }]"

[[test.results]]
value = "[{ b: 20, z: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ z: 1 }, { z: 1 }, { z: 2 }]"

[[test.results]]
value = "[{ b: 10, z: 1 }]"

[[test.results]]
value = "[{ a: 20, z: 2 }]"

[[test.results]]
value = "[{ a: 2, b: 20, c: 'y', id: t:2 }]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;
CREATE t:1 SET a = 1, b = 10, c = 'x';
CREATE t:2 SET a = 2, b = 20, c = 'y';
CREATE t:3 SET a = 1, b = 30;

SELECT * FROM (SELECT a AS z, b FROM t) WHERE z = 1 ORDER BY b;
SELECT * FROM (SELECT a, b FROM t) WHERE a = 1 AND b > 10;
SELECT * FROM (SELECT *, a AS z FROM t) WHERE z = 1 AND c = 'x';
SELECT * FROM (SELECT a = 1 AS z, b FROM t) WHERE z = true ORDER BY b;
SELECT * FROM (SELECT a = 1 AS z, b FROM t) WHERE !z ORDER BY b;
SELECT * FROM (SELECT a AS z FROM t) WHERE b = 20;
SELECT * FROM (SELECT a AS z FROM t) WHERE z = 1 OR z = 2 ORDER BY z;
SELECT * FROM (SELECT a AS z, b FROM t WHERE c = 'x') WHERE z = 1;
SELECT * FROM (SELECT a AS z, b AS a FROM t) WHERE a = 20;
SELECT * FROM (SELECT * FROM t) WHERE <int> b = 20;
