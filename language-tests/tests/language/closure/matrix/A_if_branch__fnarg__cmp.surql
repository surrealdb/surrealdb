/**
[test]
reason = "Position: a branch of an `IF` expression that holds a closure; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN IF true { [1, 5, 10, 15, 20].filter(|$e| $e >= $m) } ELSE { [] }; };
RETURN fn::f(8);
