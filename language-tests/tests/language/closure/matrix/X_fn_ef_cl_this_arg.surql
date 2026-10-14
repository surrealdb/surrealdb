/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f($m: number) { RETURN [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)]; }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
DEFINE FUNCTION fn::f($m: number) { RETURN [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)]; };
RETURN fn::f(8);
