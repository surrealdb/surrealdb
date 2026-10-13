/**
[env]
namespace = true
database = true

[test]
reason = "A closure literal nested in another reads `$parent` as the row the outer literal is written over, where nothing binds `$parent` around the outer literal, as a closure that is not nested does and as one nested in a block that binds `$parent` does: in a projection, a call of a builtin, a function argument, a field path, a destructure, a broadcast, a recursion, a computed field and a view."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [1, 10], id: t:1, lim: 8 }]"

[[test.results]]
value = "[{ a: [5, 20], id: t:2, lim: 1 }]"

[[test.results]]
value = "[{ id: rel:1, in: t:1, out: t:2, w: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[8], [1]]"

[[test.results]]
value = "[[[8]], [[1]]]"

[[test.results]]
value = "[[[8]], [[1]]]"

[[test.results]]
value = "[[[8]], [[1]]]"

[[test.results]]
value = "[[[[8]]], [[[1]]]]"

[[test.results]]
value = "[[[8]], [[1]]]"

[[test.results]]
value = "[[[8]], [[1]]]"

[[test.results]]
value = "[[[8], [8]], [[1], [1]]]"

[[test.results]]
value = "[{ z: [[8]] }, { z: [[1]] }]"

[[test.results]]
value = "[[{ z: [[6]] }, { z: [[7]] }], [{ z: [[6]] }, { z: [[7]] }]]"

[[test.results]]
value = "[{ id: t:2, z: [[1]] }, { id: t:1, z: [[8]] }]"

[[test.results]]
value = "[[[[8]]]]"

[[test.results]]
value = "[[[8]], [[1]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[[8]], [[1]]]"
*/

DEFINE FUNCTION fn::ap($f: any) { RETURN $f; };
CREATE t:1 SET lim = 8, a = [1, 10];
CREATE t:2 SET lim = 1, a = [5, 20];
RELATE t:1->rel:1->t:2 SET w = 3;
DEFINE FIELD c ON t COMPUTED [3].map(|$e| [0].map(|$f| $parent.lim));
SELECT VALUE [3].map(|$e| $parent.lim) FROM t ORDER BY id;
SELECT VALUE { LET $p = $parent; [3].map(|$e| [0].map(|$f| $p.lim)) } FROM t ORDER BY id;
SELECT VALUE [3].map(|$e| [0].map(|$f| $parent.lim)) FROM t ORDER BY id;
SELECT VALUE array::map([3], |$e| array::map([0], |$f| $parent.lim)) FROM t ORDER BY id;
SELECT VALUE [3].map(|$e| [0].map(|$f| [1].map(|$g| $parent.lim))) FROM t ORDER BY id;
SELECT VALUE [[3]].map(|$x| $x.map(|$e| $parent.lim)) FROM t ORDER BY id;
SELECT VALUE fn::ap([3].map(|$e| [0].map(|$f| $parent.lim))) FROM t ORDER BY id;
SELECT VALUE a.map(|$e| [0].map(|$f| $parent.lim)) FROM t ORDER BY id;
SELECT VALUE $this.{ z: [3].map(|$e| [0].map(|$f| $parent.lim)) } FROM t ORDER BY id;
SELECT VALUE [{ lim: 6 }, { lim: 7 }][*].{ z: [3].map(|$e| [0].map(|$f| $parent.lim)) } FROM t ORDER BY id;
SELECT id, [3].map(|$e| [0].map(|$f| $parent.lim)) AS z FROM t ORDER BY z, id;
SELECT VALUE @.{1}(->rel->t).map(|$e| [3].map(|$f| [0].map(|$g| $parent.lim))) FROM t:1;
SELECT VALUE c FROM t ORDER BY id;
DEFINE TABLE v AS SELECT [3].map(|$e| [0].map(|$f| $parent.lim)) AS z, id AS src FROM t;
SELECT VALUE z FROM v ORDER BY src;
