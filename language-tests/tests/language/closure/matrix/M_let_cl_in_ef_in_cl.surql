/**
[test]
reason = "The body is written in `RETURN array::map([[1, 10]], |$r| $r[WHERE array::any([$this], |$e| $e >= $m)])`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10]]"
*/
LET $m = 8;
RETURN array::map([[1, 10]], |$r| $r[WHERE array::any([$this], |$e| $e >= $m)]);
