/**
[test]
reason = "The body is written in `RETURN [1, 2][WHERE array::any([$this], |$e| $token = NONE)]`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[1, 2]"
*/
RETURN [1, 2][WHERE array::any([$this], |$e| $token = NONE)];
