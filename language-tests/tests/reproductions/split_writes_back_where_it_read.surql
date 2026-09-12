/**
[env]
namespace = true
database = true

[test]
reason = """
A `SPLIT` target names one position: the per-row form reads the collection
there and writes each element back over the same position. `SPLIT a[0]` on
`{ a: [[1, 2], [3, 4]] }` therefore yields `{ a: [1, [3, 4]] }` and
`{ a: [2, [3, 4]] }` — `a[1]` is untouched.

Building the write path from the target's *output name* drops every positional
step, since a name is what a projection would call the column: `a[0]`, `a[1]`
and `a[$]` all project under `a`, and writing there replaced the whole field.

`a[*]` names no single position — it reads every element — so it is declined
and the statement runs on the per-row engine.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: [1, [3, 4]], id: t:1, z: 9 }, { a: [2, [3, 4]], id: t:1, z: 9 }]"

[[test.results]]
value = "[{ a: [[1, 2], 3], id: t:1, z: 9 }, { a: [[1, 2], 4], id: t:1, z: 9 }]"

[[test.results]]
value = "[{ a: [[1, 2], 3], id: t:1, z: 9 }, { a: [[1, 2], 4], id: t:1, z: 9 }]"

[[test.results]]
value = "[{ a: [1, 2], id: t:1, z: 9 }, { a: [3, 4], id: t:1, z: 9 }]"

[[test.results]]
value = "[{ id: t:2, o: { b: [5] } }, { id: t:2, o: { b: [6] } }]"

[[test.results]]
value = "[{ a: [[1, 2], [1, 2]], id: t:1, z: 9 }, { a: [[3, 4], [3, 4]], id: t:1, z: 9 }]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = [[1, 2], [3, 4]], z = 9 RETURN NONE;
CREATE t:2 SET o = { b: [[5, 6]] } RETURN NONE;

-- A positional step is part of the position, not part of the name.
SELECT * FROM t:1 SPLIT a[0];
SELECT * FROM t:1 SPLIT a[1];
SELECT * FROM t:1 SPLIT a[$];

-- No positional step: the whole field is the position.
SELECT * FROM t:1 SPLIT a;

-- Nested under a field.
SELECT * FROM t:2 SPLIT o.b[0];

-- `a[*]` reads every element, so writing one back names no single position;
-- the statement runs on the per-row engine, which sets them all.
SELECT * FROM t:1 SPLIT a[*];
