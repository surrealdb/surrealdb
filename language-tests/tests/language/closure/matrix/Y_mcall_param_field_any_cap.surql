/**
[test]
reason = "The body is written in `RETURN $o.a.any(|$e| $e >= $m)`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
LET $o = { a: [1, 5, 10] };
LET $m = 5;
RETURN $o.a.any(|$e| $e >= $m);
