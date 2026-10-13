/**
[test]
reason = "The body is written in `RETURN array::map($a, |$e| array::len($a) + $e)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[4, 8, 13]"
*/
LET $a = [1, 5, 10];
RETURN array::map($a, |$e| array::len($a) + $e);
