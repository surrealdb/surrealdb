/**
[test]
reason = "The body is written in `IF false { RETURN 0; } ELSE IF true { RETURN [1, 5, 10, 15, 20][WHERE $this >= [0, 8, 100][$m]]; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 1;
IF false { RETURN 0; } ELSE IF true { RETURN [1, 5, 10, 15, 20][WHERE $this >= [0, 8, 100][$m]]; };
