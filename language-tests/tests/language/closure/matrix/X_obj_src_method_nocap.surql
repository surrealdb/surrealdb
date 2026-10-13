/**
[test]
reason = "The body is written in `RETURN $o.a.filter(|$e| $e >= 5)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
LET $o = { a: [1, 5, 10] };
RETURN $o.a.filter(|$e| $e >= 5);
