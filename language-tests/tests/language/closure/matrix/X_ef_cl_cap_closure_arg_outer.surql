/**
[test]
reason = "The body is written in `RETURN array::map([8], |$o| [1, 5, 10][WHERE array::any([$this], |$e| $e >= $o)])`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[[10]]"
*/
RETURN array::map([8], |$o| [1, 5, 10][WHERE array::any([$this], |$e| $e >= $o)]);
