/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20][WHERE $f($this)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $f = |$e| $e >= 8;
RETURN [1, 5, 10, 15, 20][WHERE $f($this)];
