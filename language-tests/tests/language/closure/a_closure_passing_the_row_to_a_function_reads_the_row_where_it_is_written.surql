/**
[env]
namespace = true
database = true

[test]
reason = """
A closure whose body calls a user-defined function with the row (`$this`, a field, `id`) passes the
row the literal is written over: a function that indexes with a field of its parameter
(`[0, 8, 100][$r.lim]`) answers its value for that row, as the same call does outside a closure.
The last statement is a stored field's `VALUE`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, r: [100, 100, 100] }, { id: t:2, r: [0, 0] }, { id: t:3, r: [8, 8] }]"

[[test.results]]
value = "[{ id: t:1, r: [100, 100, 100] }, { id: t:2, r: [0, 0] }, { id: t:3, r: [8, 8] }]"

[[test.results]]
value = "[{ id: t:1, r: [100] }, { id: t:2, r: [2, 9] }, { id: t:3, r: [] }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: ft:1, r: [100] }, { id: ft:2, r: [2, 9] }]"














*/
DEFINE FUNCTION fn::thr($r: any) { RETURN [0, 8, 100][$r.lim]; };
DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET lim = 2, xs = [0, 5, 100] RETURN NONE;
CREATE t:2 SET lim = 0, xs = [2, 9] RETURN NONE;
CREATE t:3 SET lim = 1, xs = [3, 0] RETURN NONE;
SELECT id, array::map(xs, |$e| fn::thr($this)) AS r FROM t ORDER BY id;
SELECT id, array::map(xs, |$e| fn::thr(id)) AS r FROM t ORDER BY id;
SELECT id, array::filter(xs, |$e| $e >= fn::thr($this)) AS r FROM t ORDER BY id;
SELECT id FROM t WHERE array::any(xs, |$e| $e >= fn::thr($this)) ORDER BY id;
DEFINE TABLE ft SCHEMALESS;
DEFINE FIELD r ON ft VALUE xs.filter(|$e| $e >= fn::thr($this));
CREATE ft:1 SET lim = 2, xs = [0, 5, 100] RETURN NONE;
CREATE ft:2 SET lim = 0, xs = [2, 9] RETURN NONE;
SELECT id, r FROM ft ORDER BY id;
