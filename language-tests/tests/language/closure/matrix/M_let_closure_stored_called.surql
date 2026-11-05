/**
[test]
reason = "The body is written in `LET $f = |$e| $e >= $m`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
LET $m = 8;
LET $f = |$e| $e >= $m;
RETURN $f(10);
