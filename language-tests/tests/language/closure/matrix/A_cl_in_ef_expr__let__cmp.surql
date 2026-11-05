/**
[test]
reason = "Position: a closure inside an element filter of an array literal; the body reads a `LET` of the statement through a comparison (`E >= $m`). It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 8;
RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)];
