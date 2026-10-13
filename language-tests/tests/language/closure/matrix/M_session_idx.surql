/**
[test]
reason = "The body is written in `RETURN [1, 2].map(|$e| [$session.ns != NONE, 0][0])`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[true, true]"
*/
RETURN [1, 2].map(|$e| [$session.ns != NONE, 0][0]);
