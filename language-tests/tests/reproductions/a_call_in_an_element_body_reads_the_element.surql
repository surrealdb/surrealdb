/**
[env]
namespace = true
database = true

[test]
reason = """
A function called in a body evaluated against an element — a `[WHERE …]`
predicate, a lookup's `[WHERE …]`, a destructure body — reads that element as
its value in scope, as a field read in the same body does: its fields, `$this`
and `$parent` are the element, whether the body only reads or also writes, and
a projection builtin reads it the same way. Called on the row itself, outside
such a body, they read the row, and a closure invoked by a builtin over the
elements runs with the row in scope. An index or a method argument of a path
step inside such a body is evaluated against the document root, not the
element, whether it reads a field itself or calls a function that does.
"""

[[test.results]]
value = "NONE"

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
value = "[[{ x: 1 }]]"

[[test.results]]
value = "[[{ x: 1 }]]"

[[test.results]]
value = "[[u:1]]"

[[test.results]]
value = "[[2, 2, 2]]"

[[test.results]]
value = "[[9, 9, 9]]"

[[test.results]]
value = "[[{ x: 1 }]]"

[[test.results]]
value = "[[[9, 9, 9], [9, 9, 9]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ enabled: true, v: 5 }]"

[[test.results]]
value = "[[{ enabled: true, v: 5 }]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[0, 1]]"

[[test.results]]
value = "[[0, 1]]"

[[test.results]]
value = "[[0, 1]]"

[[test.results]]
value = "[[0, 1]]"
*/

DEFINE FUNCTION fn::ep() { RETURN [x, $this.x, $parent.x]; };
DEFINE FUNCTION fn::ew() { UPSERT marker:1 SET n = 1; RETURN [x, $this.x, $parent.x]; };
DEFINE FUNCTION fn::enabled() { RETURN enabled; };
CREATE s:1 SET x = 9, items = [{ x: 1 }, { x: 2 }] RETURN NONE;
CREATE u:1 SET x = 7 RETURN NONE;
RELATE s:1->e->u:1 SET x = 1 RETURN NONE;
-- a `[WHERE …]` over a field, and a lookup's `[WHERE …]`
SELECT VALUE items[WHERE fn::ep() = [1, 1, 1]] FROM s;
SELECT VALUE items[WHERE fn::ew() = [1, 1, 1]] FROM s;
SELECT VALUE ->e[WHERE fn::ep() = [1, 1, 1]].out FROM s;
-- a destructure body, and the row itself
SELECT VALUE items[1].{ r: fn::ep() }.r FROM s;
SELECT VALUE fn::ep() FROM s;
-- a projection builtin reads the element too
SELECT VALUE items[WHERE type::field('x') = 1] FROM s;
-- a closure invoked by a builtin runs with the row in scope
SELECT VALUE items.map(|$i| fn::ep()) FROM s;
-- a `[WHERE …]` over a parameter, with and without a row
LET $vs = [{ enabled: false, v: 1 }, { enabled: true, v: 5 }];
RETURN $vs[WHERE fn::enabled()];
SELECT VALUE $vs[WHERE fn::enabled()] FROM s;
-- an index or a method argument of a step in such a body is evaluated against the document root
DEFINE FUNCTION fn::k() { LET $o = 0; RETURN k + $o; };
CREATE d:1 SET k = 1, items = [{ k: 0, values: [10, 20] }, { k: 1, values: [30, 40] }] RETURN NONE;
SELECT VALUE items[WHERE values[k] > 15].k FROM d:1;
SELECT VALUE items[WHERE values[fn::k()] > 15].k FROM d:1;
SELECT VALUE items[WHERE values.at(k) > 15].k FROM d:1;
SELECT VALUE items[WHERE values.at(fn::k()) > 15].k FROM d:1;
