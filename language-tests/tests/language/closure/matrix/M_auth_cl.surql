/**
[test]
reason = "The body is written in `RETURN [1, 2].map(|$e| $auth)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[NONE, NONE]"
*/
RETURN [1, 2].map(|$e| $auth);
