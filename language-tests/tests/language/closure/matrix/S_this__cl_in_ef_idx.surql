/**
[test]
reason = "The body is written in `RETURN [1, 5, 10, 15, 20][WHERE [0, 8, 100][array::len([1])] >= $this]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[1, 5]"
*/
RETURN [1, 5, 10, 15, 20][WHERE [0, 8, 100][array::len([1])] >= $this];
