/**
[env]
namespace = true
database = true

[test]
reason = "What a closure captures by value where it is written, for every source of a captured parameter, and the row it reads there; the values of a closure invoked after its captured names are rebound, shadowed or removed. A database parameter no binding shadows is the one read when the body runs."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "6"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "8"

[[test.results]]
value = "[[11], [12]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "NONE"

[[test.results]]
value = "6"

[[test.results]]
value = "4"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[11]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1, 4, 6, 'UPDATE']]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "11"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "[11]"

[[test.results]]
value = "[11]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "101"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'int' and 'none'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[11, 12]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 6, 11]"

[[test.results]]
value = "[2, 6, 11]"

[[test.results]]
value = "[1, 5, 10]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, NONE]"

[[test.results]]
value = "[20, 30, 20]"

[[test.results]]
value = "[21, 35, 30]"

[[test.results]]
value = "[21, 35, 30]"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[[1, 2, 2], [5, 6, 10], [10, 11, 20]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[8, 8, 100]"

[[test.results]]
value = "[[], [], []]"
*/

LET $m = 1;
LET $f = |$e| $e + $m;
LET $m = 100;
RETURN $f(1);
RETURN { LET $k = 5; LET $g = |$e| $e + $k; LET $k = 50; $g(1) };
DEFINE PARAM $dp VALUE 7;
LET $d = |$e| $e + $dp;
RETURN $d(1);
RETURN array::map([1, 2], |$o| array::map([10], |$i| $i + $o));
LET $outer = |$x| |$y| $x + $y;
RETURN $outer(3)(4);
DEFINE FUNCTION fn::mk($b: int) { RETURN |$e| $e + $b; };
RETURN fn::mk(5)(1);
RETURN { FOR $i IN [3] { LET $q = |$e| $e + $i; RETURN $q(1); } };
CREATE w:1 SET n = 4 RETURN NONE;
DEFINE FIELD c ON w VALUE (|$e| $e + $value)(1);
UPDATE w:1 SET c = 10 RETURN VALUE c;
DEFINE EVENT ev ON w WHEN $event = 'UPDATE' THEN { CREATE elog:1 SET v = (|$e| [$e, $before.n, $after.n, $event])(1) };
UPDATE w:1 SET n = 6 RETURN NONE;
SELECT VALUE v FROM elog;
CREATE t:1 SET n = 10 RETURN NONE;
LET $p = (SELECT VALUE (SELECT VALUE |$a| $a + $parent.n FROM [1])[0] FROM t:1)[0];
RETURN $p(1);
DEFINE FUNCTION fn::ap($f: function, $x: any) { RETURN $f($x) };
LET $m2 = 2;
RETURN fn::ap(|$e| $e + $m2, 1);
SELECT VALUE fn::ap(|$e| $e + n, 1) FROM t;
SELECT VALUE fn::ap(|$e| $e + $this.n, 1) FROM t;
LET $m3 = 8;
DEFINE FIELD f ON w2 VALUE array::filter([1, 5, 10, 15, 20], |$e| $e >= $m3);
CREATE w2:1 RETURN NONE;
SELECT VALUE f FROM w2;
DEFINE PARAM $dp2 VALUE 1;
LET $f2 = |$e| $e + $dp2;
DEFINE PARAM OVERWRITE $dp2 VALUE 100;
RETURN $f2(1);
REMOVE PARAM $dp2;
RETURN $f2(1);
LET $m4 = 1;
LET $f3 = |$x| { LET $y = $m4; LET $m4 = 5; $y + $x };
RETURN $f3(1);
LET $m5 = 1;
LET $g5 = |$x| { IF $x > 0 { LET $m5 = 7; }; $m5 + $x };
RETURN $g5(1);
LET $arr = [10, 11];
LET $s = |$x| (SELECT VALUE $this + $x FROM $arr);
RETURN $s(1);
CREATE r:1 SET n = 1 RETURN NONE;
CREATE r:2 SET n = 5 RETURN NONE;
CREATE r:3 SET n = 10 RETURN NONE;
SELECT VALUE fn::ap(|$e| $e + n, 1) FROM r ORDER BY n;
SELECT VALUE fn::ap(|$e| @.n + $e, 1) FROM r ORDER BY n;
SELECT VALUE (|$a| @.n)(1) FROM r ORDER BY n;
RETURN (|$a| (->rel))(1);
LET $l = |$a| [$a, $this];
RETURN $l(1);
SELECT VALUE fn::ap(|$e| [10, 20, 30][n % 3], 0) FROM r ORDER BY n;
SELECT VALUE fn::ap(|$e| [10, 20, 30][n % 3] + n, 0) FROM r ORDER BY n;
SELECT VALUE fn::ap(|$e| [10, 20, 30][$this.n % 3] + $this.n, 0) FROM r ORDER BY n;
SELECT VALUE n FROM r WHERE fn::ap(|$e| $e + n, 1) > 3 ORDER BY n;
SELECT VALUE [n, fn::ap(|$e| $e + n, 1), fn::ap(|$e| $e * n, 2)] FROM r ORDER BY n;
DEFINE FUNCTION fn::lim() { RETURN $this.lim };
CREATE ql:1 SET n = 1, lim = 1 RETURN NONE;
CREATE ql:2 SET n = 5, lim = 1 RETURN NONE;
CREATE ql:3 SET n = 10, lim = 2 RETURN NONE;
SELECT VALUE (|$e| [0, 8, 100][fn::lim()])(1) FROM ql ORDER BY n;
SELECT VALUE [n].filter(|$e| $e >= [0, 8, 100][fn::lim()]) FROM ql ORDER BY n;
