/**
[test]
reason = "The body is written in `RETURN [[1, 10]][WHERE array::any($this[WHERE $this >= $m], |$e| true)]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 10]]"
*/
LET $m = 8;
RETURN [[1, 10]][WHERE array::any($this[WHERE $this >= $m], |$e| true)];
