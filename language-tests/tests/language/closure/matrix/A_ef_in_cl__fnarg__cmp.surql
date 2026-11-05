/**
[test]
reason = "Position: an element filter inside a closure of `array::map`; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], [10, 15, 20]]"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN array::map([[1, 5], [10, 15, 20]], |$r| $r[WHERE $this >= $m]); };
RETURN fn::f(8);
