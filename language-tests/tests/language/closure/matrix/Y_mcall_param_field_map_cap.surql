/**
[test]
reason = "The body is written in `RETURN $o.a.map(|$e| $e + $m)`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6, 10, 15]"
*/
LET $o = { a: [1, 5, 10] };
LET $m = 5;
RETURN $o.a.map(|$e| $e + $m);
