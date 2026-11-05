/**
[test]
reason = "The body is written in `RETURN (SELECT VALUE $m FROM [1])`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[8]"
*/
LET $m = 8;
RETURN (SELECT VALUE $m FROM [1]);
