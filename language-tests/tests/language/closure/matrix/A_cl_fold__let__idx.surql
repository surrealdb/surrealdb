/**
[test]
reason = "Position: the closure of `array::fold`; the body reads a `LET` of the statement through an index argument (`E >= [0, 8, 100][$m]`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"
*/
LET $m = 1;
RETURN array::fold([1, 5, 10, 15, 20], 0, |$a, $e| IF $e >= [0, 8, 100][$m] { $a + 1 } ELSE { $a });
