/**
[test]
reason = "Position: the closure of the `.any` method; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN [1, 5, 10, 15, 20].any(|$e| $e >= $m); };
RETURN fn::f(8);
