/**
[test]
reason = "Position: the condition of an `IF` expression that holds an element filter; the body reads a `LET` of the statement through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'y'"
*/
LET $m = 8;
RETURN IF [10][WHERE $this >= $m] != [] { 'y' } ELSE { 'n' };
