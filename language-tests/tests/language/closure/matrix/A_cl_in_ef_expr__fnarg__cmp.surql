/**
[test]
reason = "Position: a closure inside an element filter of an array literal; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)]; };
RETURN fn::f(8);
