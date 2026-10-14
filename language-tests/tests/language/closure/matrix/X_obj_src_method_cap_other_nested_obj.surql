/**
[test]
reason = "The body is written in `RETURN $o.a.b.filter(|$e| $e >= $m)`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
LET $o = { a: { b: [1, 5, 10] } };
LET $m = 5;
RETURN $o.a.b.filter(|$e| $e >= $m);
