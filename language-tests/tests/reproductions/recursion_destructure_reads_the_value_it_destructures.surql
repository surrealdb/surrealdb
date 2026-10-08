/**
[env]
namespace = true
database = true

[test]
reason = """
Within an aliased field of a destructure in a recursion body, the document
is the value destructured: `$parent`, a statement reading `$parent`, and a
closure written or invoked there all read the record of the depth being
destructured, and a body that yields no record raises the recursion's error.
"""

[[test.results]]
value = "[{ a: 1, id: t:1, n: t:2 }]"

[[test.results]]
value = "[{ a: 2, id: t:2, n: t:3 }]"

[[test.results]]
value = "[{ a: 3, id: t:3 }]"

[[test.results]]
value = "[{ a: 10, id: u:1, n: t:1 }]"

[[test.results]]
value = "[{ r: { id: t:1, next: { id: t:2, next: { id: t:3, next: NONE, p: 3 }, p: 2 }, p: 1 } }]"

[[test.results]]
value = "{ id: t:1, next: { id: t:2, next: { id: t:3, next: NONE, p: { a: 3, id: t:3 } }, p: { a: 2, id: t:2, n: t:3 } }, p: { a: 1, id: t:1, n: t:2 } }"

[[test.results]]
value = "[{ r: { g: 1, id: t:1, next: { g: 2, id: t:2, next: { g: 3, id: t:3, next: NONE } } } }]"

[[test.results]]
value = "[{ r: { c: 2, id: t:1, next: { c: 1, id: t:2, next: { c: 0, id: t:3, next: NONE } } } }]"

[[test.results]]
value = "[{ r: { g: 2, id: t:1, next: { g: 3, id: t:2, next: { g: 4, id: t:3, next: NONE } } } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ r: { g: 10, id: t:1, next: { g: 20, id: t:2, next: { g: 30, id: t:3, next: NONE } } } }]"

[[test.results]]
error = "Expected a record ID during recursive traversal, but found `{ id: t:1, p: { a: 1, id: t:1, n: t:2 } }`"

[[test.results]]
error = "Expected a record ID during recursive traversal, but found `{ f: |$x: any| $x + a, id: t:1 }`"
*/
CREATE t:1 SET a = 1, n = t:2; CREATE t:2 SET a = 2, n = t:3; CREATE t:3 SET a = 3; CREATE u:1 SET n = t:1, a = 10;
SELECT n.{..3}.{ id, p: $parent.a, next: n.@ } AS r FROM u:1;
RETURN t:1.{..3}.{ id, p: $parent, next: n.@ };
SELECT @.{..3}.{ id, g: (SELECT VALUE a FROM ONLY $parent), next: n.@ } AS r FROM t:1;
SELECT @.{..3}.{ id, c: count((SELECT * FROM t WHERE a > $parent.a)), next: n.@ } AS r FROM t:1;
SELECT @.{..3}.{ id, g: (|$x| $x + a)(1), next: n.@ } AS r FROM t:1;
LET $g = |$x| $x * 10;
SELECT @.{..3}.{ id, g: $g(a), next: n.@ } AS r FROM t:1;
SELECT * FROM t:1.{..3}.{ id, p: $parent };
SELECT @.{..3}.{ id, f: |$x| $x + a } AS r FROM t:1;
