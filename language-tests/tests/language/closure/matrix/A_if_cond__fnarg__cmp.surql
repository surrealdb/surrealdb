/**
[test]
reason = "Position: the condition of an `IF` expression that holds a closure; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'y'"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN IF array::any([1, 5, 10, 15, 20], |$e| $e >= $m) { 'y' } ELSE { 'n' }; };
RETURN fn::f(8);
