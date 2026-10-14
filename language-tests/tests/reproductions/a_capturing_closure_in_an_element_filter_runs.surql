/**
[env]
namespace = true
database = true

[test]
reason = "A closure written in an element filter runs and reads, for every source a captured name or row can come from, the value it has where the literal is written: a statement LET, a DEFINE PARAM, a function argument, a FOR variable, an outer closure's argument, $this, an element field, $parent, $value, $after, a stored closure, the document root an index argument reads, and the document root an element filter inside the closure body binds as its `$parent` (the literal captures it even where an enclosing filter binds `$parent` around the literal). The session names are read when the closure runs. A block-bodied closure handed to a builtin in an element body runs over the element, so its `$this` is the element in a `SELECT` as in a `RETURN`; one handed to a method runs over the row. A bare field in a closure that a method of the element's field runs reads the frame's row, not the element."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10]]"

[[test.results]]
value = "[[10]]"

[[test.results]]
value = "[5, 10, 15, 20]"

[[test.results]]
value = "[{ k: 8, v: 10 }]"

[[test.results]]
value = "[{ id: t:1, lim: 8, n: 1 }]"

[[test.results]]
value = "[{ id: u:1, m: 1 }]"

[[test.results]]
value = "[{ id: u:2, m: 5 }]"

[[test.results]]
value = "[{ id: u:3, m: 10 }]"

[[test.results]]
value = "[{ n: 1, r: [1, 5, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [10, 15, 20], id: w:1 }]"

[[test.results]]
value = "[[10, 15, 20]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 8 }]"

[[test.results]]
value = "[[10, 15, 20]]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"

[[test.results]]
value = "[10, 15, 20]"

[[test.results]]
value = "[10, 15, 20]"

[[test.results]]
value = "[{ id: p:1, k: 9 }]"

[[test.results]]
value = "[[{ k: 1 }, { k: 2 }]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[{ id: r:1, lim: 1, n: 10 }]"

[[test.results]]
value = "[{ id: r:2, lim: 1, n: 5 }]"

[[test.results]]
value = "[[], [10]]"

[[test.results]]
value = "[{ id: s:1, lim: 8, n: 10 }]"

[[test.results]]
value = "[[10]]"

[[test.results]]
value = "[{ id: b:1, k: 9, n: 1 }]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[{ a: [1, 10], k: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: [1, 5, 10, 15, 20], id: w2:1, lim: 8 }]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[], [], []]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], [], [10]]"

[[test.results]]
value = "[10]"

[[test.results]]
value = "[[], [], []]"

[[test.results]]
value = "[[], [], [10]]"

[[test.results]]
value = "[[], [], [10]]"

[[test.results]]
value = "[10]"
*/

LET $m = 8;
RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)];
DEFINE PARAM $dp VALUE 8;
RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $dp)];
DEFINE FUNCTION fn::between($a: number, $b: number) { RETURN [1, 5, 10, 15][WHERE array::any([$this], |$e| $e >= $a AND $e <= $b)]; };
RETURN fn::between(5, 10);
FOR $fm IN [8] { CREATE out:1 SET v = [1, 5, 10][WHERE array::any([$this], |$e| $e >= $fm)]; };
SELECT VALUE v FROM out;
RETURN array::map([8], |$o| [1, 5, 10][WHERE array::any([$this], |$e| $e >= $o)]);
RETURN [1, 5, 10, 15, 20][WHERE array::any([1, 2], |$e| $e < $this)];
RETURN [{ k: 8, v: 10 }, { k: 8, v: 1 }][WHERE array::any([v], |$e| $e >= k)];
CREATE t:1 SET n = 1, lim = 8;
CREATE u:1 SET m = 1;
CREATE u:2 SET m = 5;
CREATE u:3 SET m = 10;
SELECT n, (SELECT VALUE m FROM u WHERE [m][WHERE array::any([$this], |$e| $e >= $parent.lim)] != [] ORDER BY m) AS r FROM t:1;
DEFINE FIELD f ON w VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $value)];
CREATE w:1 SET f = 8;
SELECT VALUE f FROM w;
DEFINE EVENT ev ON src WHEN true THEN { CREATE log:1 SET v = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $after.lim)] };
CREATE src:1 SET lim = 8;
SELECT VALUE v FROM log;
RETURN [1, 2][WHERE array::any([$this], |$e| $session.ns != NONE)];
RETURN [1, 2][WHERE array::any([$this], |$e| $auth = NONE)];
RETURN [1, 2][WHERE array::any([$this], |$e| $token = NONE)];
LET $f = |$e| $e >= 8;
RETURN [1, 5, 10, 15, 20][WHERE $f($this)];
RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= 8)];
RETURN [1, 5, 10, 15, 20][WHERE $this >= 8];
CREATE p:1 SET k = 9;
SELECT VALUE [{ k: 1 }, { k: 2 }][WHERE (SELECT VALUE $parent.k FROM ONLY [1] LIMIT 1) = 9] FROM p;
SELECT VALUE [{ k: 1 }, { k: 2 }][WHERE (SELECT VALUE $parent.k FROM ONLY [1] LIMIT 1) = 2] FROM p;
CREATE r:1 SET n = 10, lim = 1;
CREATE r:2 SET n = 5, lim = 1;
SELECT VALUE [n][WHERE array::any([$this], |$e| $e >= [0, 8, 100][lim])] FROM r ORDER BY n;
CREATE s:1 SET n = 10, lim = 8;
SELECT VALUE [n, 3][WHERE [$this].filter(|$e| $e >= lim) != []] FROM s;
CREATE b:1 SET k = 9, n = 1;
SELECT VALUE [1, 2][WHERE array::any([1], |$e| { RETURN $this == 2 })] FROM b;
RETURN [1, 2][WHERE array::any([1], |$e| { RETURN $this == 2 })];
RETURN [{ a: [1, 10], k: 5 }][WHERE a.filter(|$e| $e >= k) != []];
DEFINE FIELD g ON w2 VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= lim)];
CREATE w2:1 SET lim = 8;
SELECT VALUE g FROM w2;
RETURN [1, 2][WHERE [1].any(|$e| { RETURN $this == 2 })];
RETURN [1, 2].map(|$a| { LET $y = $a; RETURN $y + 1 });
SELECT * FROM [{ a: 1 }, { a: 2 }] WHERE [1, 2].map(|$x| { LET $y = $x; RETURN $y + 1 }) = [2, 3];
SELECT VALUE [1, 2][WHERE [1].any(|$e| { RETURN $this == 2 })] FROM b;
SELECT VALUE [1, 2][WHERE array::any([1], |$e| { LET $y = $e; RETURN $y + 1 } = 2)] FROM b;
DEFINE FUNCTION fn::lim() { RETURN $this.lim };
CREATE ql:1 SET n = 1, lim = 1, k = 9 RETURN NONE;
CREATE ql:2 SET n = 5, lim = 1, k = 1 RETURN NONE;
CREATE ql:3 SET n = 10, lim = 2, k = 1 RETURN NONE;
SELECT VALUE [n][WHERE array::any([$this], |$e| $e >= [0, 8, 100][fn::lim()])] FROM ql ORDER BY n;
DEFINE FUNCTION fn::ap($f: function, $x: any) { RETURN $f($x) };
SELECT VALUE [n][WHERE array::any([1], |$e| [1][WHERE $parent.lim = 2] != [])] FROM ql ORDER BY n;
SELECT VALUE n FROM ql WHERE [n][WHERE array::any([1], |$e| [1][WHERE $parent.lim = 2] != [])] != [] ORDER BY n;
SELECT VALUE [1, 2][WHERE fn::ap(|$e| [1][WHERE $parent.k = 9] != [], 0)] FROM ql ORDER BY n;
SELECT VALUE [n][WHERE array::any([1], |$e| [1][WHERE $parent.lim = 2] != [] AND [true][@.zz] = NONE)] FROM ql ORDER BY n;
SELECT VALUE [n][WHERE [1][WHERE $parent.lim = 2] != []] FROM ql ORDER BY n;
SELECT VALUE n FROM ql WHERE [n][WHERE [1][WHERE $parent.lim = 2] != []] != [] ORDER BY n;
