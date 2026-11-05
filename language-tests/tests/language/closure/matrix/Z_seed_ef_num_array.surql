/**
[test]
reason = "The body is written in `RETURN [[95], [10]][WHERE array::any($this, |$e| $e >= $min)]`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[95]]"
*/
LET $min = 90;
RETURN [[95], [10]][WHERE array::any($this, |$e| $e >= $min)];
