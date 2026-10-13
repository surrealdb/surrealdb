/**
[test]
reason = "Position: the closure of `array::map`; the body reads a `LET` of the statement through an index argument (`E >= [0, 8, 100][$m]`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[false, false, true, true, true]"
*/
LET $m = 1;
RETURN array::map([1, 5, 10, 15, 20], |$e| $e >= [0, 8, 100][$m]);
