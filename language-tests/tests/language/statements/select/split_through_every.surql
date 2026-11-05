/**
[env]
namespace = true
database = true

[test]
reason = """
A `SPLIT` target that reaches through every element of a collection
(`a[*].b`) names one position in each element. The row is split on the values
read there, and each element of the split takes the value at its own position,
so every element of `a` carries it. The same holds when the reaching step is
`.*`, when several of them are chained, and when the target ends in one.
"""

[[test.results]]
value = "NONE"

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
value = "[{ a: [{ b: 1 }, { b: 1 }], id: t:1, z: 9 }, { a: [{ b: 2 }, { b: 2 }], id: t:1, z: 9 }]"

[[test.results]]
value = "[{ a: [{ b: 1 }, { b: 1 }], id: t:1, z: 9 }, { a: [{ b: 2 }, { b: 2 }], id: t:1, z: 9 }]"

[[test.results]]
value = "[{ a: [{ b: [1, 2] }, { b: [1, 2] }], id: t:2 }, { a: [{ b: [3] }, { b: [3] }], id: t:2 }]"

[[test.results]]
value = "[{ a: [{ b: [{ c: [1, 2] }, { c: [1, 2] }] }, { b: [{ c: [1, 2] }] }], id: t:3 }, { a: [{ b: [{ c: [3] }, { c: [3] }] }, { b: [{ c: [3] }] }], id: t:3 }]"

[[test.results]]
value = "[{ a: [{ b: [[{ c: 1 }, { c: 2 }], [{ c: 1 }, { c: 2 }]] }, { b: [[{ c: 1 }, { c: 2 }]] }], id: t:3 }, { a: [{ b: [[{ c: 3 }], [{ c: 3 }]] }, { b: [[{ c: 3 }]] }], id: t:3 }]"

[[test.results]]
value = "[{ a: 5, id: t:5 }]"

[[test.results]]
value = "[{ a: { x: { b: 1 }, y: { b: 1 } }, id: t:6 }, { a: { x: { b: 2 }, y: { b: 2 } }, id: t:6 }]"

[[test.results]]
value = "[{ a: [{ b: 1 }, { b: 1 }], id: t:1, z: 9 }, { a: [{ b: 2 }, { b: 2 }], id: t:1, z: 9 }]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = [{ b: 1 }, { b: 2 }], z = 9 RETURN NONE;
CREATE t:2 SET a = [{ b: [1, 2] }, { b: [3] }] RETURN NONE;
CREATE t:3 SET a = [{ b: [{ c: 1 }, { c: 2 }] }, { b: [{ c: 3 }] }] RETURN NONE;
CREATE t:5 SET a = 5 RETURN NONE;
CREATE t:6 SET a = { x: { b: 1 }, y: { b: 2 } } RETURN NONE;

SELECT * FROM t:1 SPLIT a[*].b;
SELECT * FROM t:1 SPLIT a.*.b;
SELECT * FROM t:2 SPLIT a[*].b;
SELECT * FROM t:3 SPLIT a[*].b[*].c;
SELECT * FROM t:3 SPLIT a[*].b[*];
SELECT * FROM t:5 SPLIT a[*].b;
SELECT * FROM t:6 SPLIT a[*].b;
SELECT * FROM t:1 SPLIT a[*].b, z;
