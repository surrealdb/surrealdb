/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f() { RETURN [1, 5, 10][WHERE array::any([$this], |$e| $e >= 8)]; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
DEFINE FUNCTION fn::f() { RETURN [1, 5, 10][WHERE array::any([$this], |$e| $e >= 8)]; };
RETURN fn::f();
