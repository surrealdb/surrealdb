/**
[test]
reason = "The body is written in `RETURN $o.a.map(|$e| $e + 1)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 6, 11]"
*/
LET $o = { a: [1, 5, 10] };
RETURN $o.a.map(|$e| $e + 1);
