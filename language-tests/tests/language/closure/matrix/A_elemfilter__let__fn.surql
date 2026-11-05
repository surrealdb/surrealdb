/**
[test]
reason = "Position: an element filter `[WHERE ..]` of an array literal; the body reads a `LET` of the statement through a builtin call (`math::max([E, $m]) = E`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 8;
RETURN [1, 5, 10, 15, 20][WHERE math::max([$this, $m]) = $this];
