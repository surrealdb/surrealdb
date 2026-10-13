/**
[test]
reason = "Position: the condition of an `IF` expression that holds a closure; the body reads a `LET` of the statement through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'y'"
*/
LET $m = 8;
RETURN IF array::any([1, 5, 10, 15, 20], |$e| $e >= $m) { 'y' } ELSE { 'n' };
