/**
[test]
reason = "The body is written in `RETURN [1, 2].filter(|$e| $session.ns != NONE)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[1, 2]"
*/
RETURN [1, 2].filter(|$e| $session.ns != NONE);
