/**
[test]
reason = "The body is written in `LET $f = |$e| $e >= $m`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
LET $m = 8;
LET $f = |$e| $e >= $m;
LET $m = 100;
RETURN $f(10);
