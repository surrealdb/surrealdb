/**
[test]
reason = "The body is written in `RETURN [1, 2][WHERE array::any([$this], |$e| $auth = NONE)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[1, 2]"
*/
RETURN [1, 2][WHERE array::any([$this], |$e| $auth = NONE)];
