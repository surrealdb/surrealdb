/**
[env]
namespace = true
database = true

[test]
reason = "A closure that captures a name and is the argument of a method called on a path with a field step, `$o.a.filter(|$e| $e >= $m)`, runs and reads the captured value."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[6, 10, 15]"

[[test.results]]
value = "true"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[1, 5, 10]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "[{ id: x:1, v: [1, 5, 10] }]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "[[[5, 10]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [5, 10] }]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [1, 5, 10], id: src:1 }]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[2, 6, 11]"
*/

LET $o = { a: [1, 5, 10] };
LET $m = 5;
RETURN $o.a.filter(|$e| $e >= $m);
RETURN $o.a.map(|$e| $e + $m);
RETURN $o.a.any(|$e| $e >= $m);
RETURN $o.a.filter(|$e| $e >= $o.a[1]);
RETURN $o.a.filter(|$e| $e >= $o.v);
LET $n = { a: { b: [1, 5, 10] } };
RETURN $n.a.b.filter(|$e| $e >= $m);
SELECT VALUE a.b.filter(|$e| $e >= $m) FROM [{ a: { b: [1, 5, 10] } }];
CREATE x:1 SET v = [1, 5, 10];
SELECT VALUE $this.v.filter(|$e| $e >= $m) FROM x:1;
SELECT VALUE (SELECT VALUE $parent.v.filter(|$e| $e >= $m) FROM [1]) FROM x:1;
DEFINE PARAM $dm VALUE 5;
DEFINE FIELD s ON w VALUE $value.a.filter(|$e| $e >= $dm);
CREATE w:1 SET s = { a: [1, 5, 10] };
SELECT VALUE s FROM w;
DEFINE FUNCTION fn::f($o: object, $q: number) { RETURN $o.a.filter(|$e| $e >= $q); };
RETURN fn::f({ a: [1, 5, 10] }, 5);
RETURN array::map([{ a: [1, 5, 10] }], |$g| $g.a.filter(|$e| $e >= 5 AND $e >= $g.a[0]));
RETURN array::map([{ a: [1, 5, 10], k: 5 }], |$g| $g.a.filter(|$e| $e >= $g.k));
RETURN array::map([{ a: [1, 5, 10] }], |$g| $g.a.filter(|$e| $e >= $m));
DEFINE EVENT ev ON src WHEN true THEN { CREATE log:1 SET v = $after.a.filter(|$e| $e >= $dm) };
CREATE src:1 SET a = [1, 5, 10];
SELECT VALUE v FROM log;
LET $a = [1, 5, 10];
RETURN $a.filter(|$e| $e >= $m);
LET $o2 = [[1, 5, 10]];
RETURN $o2[0].filter(|$e| $e >= $m);
RETURN array::filter($o.a, |$e| $e >= $m);
RETURN $o.a.map(|$e| $e + 1);
