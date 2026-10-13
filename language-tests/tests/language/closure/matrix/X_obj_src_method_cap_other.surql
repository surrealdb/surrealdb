/**
[test]
reason = "The body is written in `RETURN $o.a.filter(|$e| $e >= $m)`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
LET $o = { a: [1, 5, 10] };
LET $m = 5;
RETURN $o.a.filter(|$e| $e >= $m);
