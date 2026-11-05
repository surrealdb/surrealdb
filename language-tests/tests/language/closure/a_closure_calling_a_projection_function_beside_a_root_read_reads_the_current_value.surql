/**
[env]
namespace = true
database = true

[test]
reason = "A closure whose body reads the current value through a projection function (`type::field`, `type::fields`) and the document root through an index argument runs, in a projection, invoked at once and in a subquery: the projection function reads the value the closure was written over, as `$this` does. A projection function in a destructure the body holds reads the value destructured, as it does in a body with no closure."

[[test.results]]
value = "[{ id: t:1, k: 1, n: 10, x: 99, xs: [1, 2, 3] }]"

[[test.results]]
value = "[{ id: t:2, k: 0, n: 20, x: 99, xs: [4] }]"

[[test.results]]
value = "[[30], [30]]"

[[test.results]]
value = "[30, 30]"

[[test.results]]
value = "[[22], [12]]"

[[test.results]]
value = "[[[10, 10, 20]], [[20, 20, 10]]]"

[[test.results]]
value = "[[[10, 20, [{ q: 7, r: 7 }, { q: 8, r: 8 }]]], [[20, 10, [{ q: 7, r: 7 }, { q: 8, r: 8 }]]]]"

[[test.results]]
value = "[[10, 20, [{ q: 7 }]], [20, 10, [{ q: 7 }]]]"

[[test.results]]
value = "[[[10, 20, [{ q: 7 }]]], [[20, 10, [{ q: 7 }]]]]"

[[test.results]]
value = "[[[10, 20, [{ q: { z: 5 } }]]], [[20, 10, [{ q: { z: 5 } }]]]]"

[[test.results]]
value = "[[[10, 20, { q: 7 }]], [[20, 10, { q: 7 }]]]"

[[test.results]]
value = "[[10, 20, [{ q: 7 }, { q: 8 }]], [20, 10, [{ q: 7 }, { q: 8 }]]]"
*/

CREATE t:1 SET n = 10, k = 1, x = 99, xs = [1, 2, 3];
CREATE t:2 SET n = 20, k = 0, x = 99, xs = [4];
SELECT VALUE [1].map(|$e| type::field('n') + [10, 20][k]) FROM t ORDER BY id;
SELECT VALUE (|$e| type::field('n') + [10, 20][k])(1) FROM t ORDER BY id;
RETURN (SELECT VALUE [1].map(|$e| type::fields(['n', 'k']).len() + [10, 20][k]) FROM t ORDER BY id);
SELECT VALUE [1].map(|$e| [type::field('n'), $this.n, [10, 20][k]]) FROM t ORDER BY id;
SELECT VALUE [1].map(|$a| [n, [10, 20][k], [{x: 7}, {x: 8}].{ q: type::field('x'), r: x }]) FROM t ORDER BY id;
SELECT VALUE (|$a| [n, [10, 20][k], [{x: 7}].{ q: type::field('x') }])(1) FROM t ORDER BY id;
SELECT VALUE [1].map(|$a| [n, [10, 20][k], [{x: 7}].{ q: type::fields(['x']) }]) FROM t ORDER BY id;
SELECT VALUE [1].map(|$a| [n, [10, 20][k], [{x: {y: 5}}].{ q: x.{ z: type::field('y') } }]) FROM t ORDER BY id;
SELECT VALUE [1].map(|$a| [n, [10, 20][k], {x: 7}.{ q: type::field('x') }]) FROM t ORDER BY id;
SELECT VALUE [n, [10, 20][k], [{x: 7}, {x: 8}].{ q: type::field('x') }] FROM t ORDER BY id;
