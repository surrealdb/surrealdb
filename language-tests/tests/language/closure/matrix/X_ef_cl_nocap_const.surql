/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= 8)]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[10, 15, 20]"
*/
RETURN [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= 8)];
