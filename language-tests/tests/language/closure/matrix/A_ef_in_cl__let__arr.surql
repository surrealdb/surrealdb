/**
[test]
reason = "Position: an element filter inside a closure of `array::map`; the body reads a `LET` of the statement through an array index (`E >= $m[1]`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], [10, 15, 20]]"
*/
LET $m = [0, 8, 100];
RETURN array::map([[1, 5], [10, 15, 20]], |$r| $r[WHERE $this >= $m[1]]);
