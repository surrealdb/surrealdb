/**
[env]
namespace = true
database = true

[test]
reason = """
An OMIT path whose field step meets an array removes the field from each of its
elements, nested arrays included, as a path delete does: OMIT
o.b over o = [{ b, c }, ...] leaves [{ c }, ...]. This holds under SELECT VALUE,
SELECT * and a field projection alike, for a destructure, and for a field
reached through an array; an index, a filter and .* name their elements as
before.
"""

[[test.results]]
value = "[{ id: o:1, n: [[{ b: 1, c: 2 }], { b: 3 }], o: [{ b: 1, c: 2 }, { b: 2, c: 3 }], p: { b: 1, c: 2 }, q: [{ x: { b: 1, c: 2 } }] }]"

[[test.results]]
value = "[[{ c: 2 }, { c: 3 }]]"

[[test.results]]
value = "[{ id: o:1, n: [[{ b: 1, c: 2 }], { b: 3 }], o: [{ c: 2 }, { c: 3 }], p: { b: 1, c: 2 }, q: [{ x: { b: 1, c: 2 } }] }]"

[[test.results]]
value = "[{ o: [{ c: 2 }, { c: 3 }] }]"

[[test.results]]
value = "[[[{ c: 2 }], {  }]]"

[[test.results]]
value = "[[{  }, {  }]]"

[[test.results]]
value = "[[{ x: { c: 2 } }]]"

[[test.results]]
value = "[{ c: 2 }]"

[[test.results]]
value = "[[{ c: 2 }, { b: 2, c: 3 }]]"

[[test.results]]
value = "[[{ b: 1 }, { b: 2, c: 3 }]]"

[[test.results]]
value = "[[{ c: 2 }, { c: 3 }]]"

*/

CREATE o:1 SET o = [{ b: 1, c: 2 }, { b: 2, c: 3 }], n = [[{ b: 1, c: 2 }], { b: 3 }], p = { b: 1, c: 2 }, q = [{ x: { b: 1, c: 2 } }];
SELECT VALUE o OMIT o.b FROM o;
SELECT * OMIT o.b FROM o;
SELECT o OMIT o.b FROM o;
SELECT VALUE n OMIT n.b FROM o;
SELECT VALUE o OMIT o.{ b, c } FROM o;
SELECT VALUE q OMIT q.x.b FROM o;
SELECT VALUE p OMIT p.b FROM o;
SELECT VALUE o OMIT o[0].b FROM o;
SELECT VALUE o OMIT o[WHERE b = 1].c FROM o;
SELECT VALUE o OMIT o.*.b FROM o;
