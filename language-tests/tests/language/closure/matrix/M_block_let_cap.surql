/**
[test]
reason = "The body is written in `RETURN { LET $m = 8; [1, 5, 10, 15].filter(|$e| $e >= $m) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[10, 15]"
*/
RETURN { LET $m = 8; [1, 5, 10, 15].filter(|$e| $e >= $m) };
