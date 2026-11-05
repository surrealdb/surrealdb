/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f($a: number, $b: number) { RETURN [1, 5, 10, 15][WHERE array::any([$this], |$e| $e >= $a AND $e <= $b)]; }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
DEFINE FUNCTION fn::f($a: number, $b: number) { RETURN [1, 5, 10, 15][WHERE array::any([$this], |$e| $e >= $a AND $e <= $b)]; };
RETURN fn::f(5, 10);
