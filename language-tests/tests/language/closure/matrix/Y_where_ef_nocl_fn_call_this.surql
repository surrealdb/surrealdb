/**
[test]
reason = "The body is written in `RETURN [1, 5, 10][WHERE array::len([$this]) = 1]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[1, 5, 10]"
*/
RETURN [1, 5, 10][WHERE array::len([$this]) = 1];
