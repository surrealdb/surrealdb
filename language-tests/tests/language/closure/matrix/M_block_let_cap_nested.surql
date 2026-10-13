/**
[test]
reason = "The body is written in `RETURN { LET $m = 8; { LET $n = 3; [1, 5, 10, 15].filter(|$e| $e >= $m AND $e >= $n) } }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[10, 15]"
*/
RETURN { LET $m = 8; { LET $n = 3; [1, 5, 10, 15].filter(|$e| $e >= $m AND $e >= $n) } };
