/**
[test]
reason = "The body is written in `RETURN array::map([8], |$o| [1, 5, 10][WHERE array::any([$this], |$e| $e >= $o)])`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[[10]]"
*/
RETURN array::map([8], |$o| [1, 5, 10][WHERE array::any([$this], |$e| $e >= $o)]);
