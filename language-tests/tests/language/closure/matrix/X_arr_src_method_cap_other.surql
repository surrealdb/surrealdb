/**
[test]
reason = "The body is written in `RETURN $a.filter(|$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
LET $a = [1, 5, 10];
LET $m = 5;
RETURN $a.filter(|$e| $e >= $m);
