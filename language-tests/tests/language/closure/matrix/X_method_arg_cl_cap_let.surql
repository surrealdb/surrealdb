/**
[test]
reason = "The body is written in `RETURN [1, 5, 10].filter(|$e| $e >= $m).map(|$e| $e + $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[18]"
*/
LET $m = 8;
RETURN [1, 5, 10].filter(|$e| $e >= $m).map(|$e| $e + $m);
