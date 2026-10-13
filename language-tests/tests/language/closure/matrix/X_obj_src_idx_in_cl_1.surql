/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15].filter(|$e| $e >= $o.a[1])`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10, 15]"
*/
LET $o = { a: [1, 5, 10] };
RETURN [1, 5, 10, 15].filter(|$e| $e >= $o.a[1]);
