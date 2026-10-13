/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20].filter(|$e| $e >= $o.v)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $o = { v: 8 };
RETURN [1, 5, 10, 15, 20].filter(|$e| $e >= $o.v);
