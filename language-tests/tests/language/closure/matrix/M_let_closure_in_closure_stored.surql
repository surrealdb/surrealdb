/**
[test]
reason = "The body is written in `LET $f = |$a| [1, 5, 10, 15].filter(|$e| $e >= $m AND $e >= $a)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15]"
*/
LET $m = 8;
LET $f = |$a| [1, 5, 10, 15].filter(|$e| $e >= $m AND $e >= $a);
RETURN $f(1);
