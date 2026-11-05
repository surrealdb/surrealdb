/**
[test]
reason = "The body is written in `RETURN array::map([1, 5, 10], |$o| [1, 5, 10, 15][WHERE array::any([$this], |$q| $q >= $o)])`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[[1, 5, 10, 15], [5, 10, 15], [10, 15]]"
*/
RETURN array::map([1, 5, 10], |$o| [1, 5, 10, 15][WHERE array::any([$this], |$q| $q >= $o)]);
