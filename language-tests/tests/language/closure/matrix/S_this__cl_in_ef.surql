/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20][WHERE array::any([1, 2], |$e| $e < $this)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[5, 10, 15, 20]"
*/
RETURN [1, 5, 10, 15, 20][WHERE array::any([1, 2], |$e| $e < $this)];
