/**
[test]
reason = "Position: an element filter `[WHERE ..]` of an array literal; the body reads a `LET` of the statement through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 8;
RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= $m)];
