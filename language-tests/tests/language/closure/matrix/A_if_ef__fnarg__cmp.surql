/**
[test]
reason = "Position: the condition of an `IF` expression that holds an element filter; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'y'"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN IF [10][WHERE $this >= $m] != [] { 'y' } ELSE { 'n' }; };
RETURN fn::f(8);
