/**
[env]
namespace = true
database = true

[test]
reason = """
A place whose path holds a part no address follows (an optional step, a
flatten, a destructure, a recursion, a method call) writes nothing and removes
nothing: the walk stops where it lands. The record is not widened to the
prefix of the path, and the other assignments of the same clause still write.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, c: 8, id: t:1 }]"

[[test.results]]
value = "[{ a: { b: 1 }, c: 8, id: t:1 }]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET a = { b: 1 } RETURN NONE;

UPDATE t:1 SET a.b.? = 2;
UPDATE t:1 SET a... = 3;
UPDATE t:1 SET a.{b} = 4;
UPDATE t:1 SET a.b.@ = 6;
UPDATE t:1 UNSET a.upper();
UPDATE t:1 UNSET a.b.?;
UPDATE t:1 UNSET a...;
UPDATE t:1 UNSET a.{b};
UPDATE t:1 UNSET a.b.@;
UPDATE t:1 SET a.b.? = 7, c = 8;
SELECT * FROM t:1;
