/**
[test]
reason = "The body is written in `LET $r = [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)]`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
LET $m = 8;
LET $r = [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)];
RETURN $r;
