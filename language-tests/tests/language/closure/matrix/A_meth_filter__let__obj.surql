/**
[test]
reason = "Position: the closure of the `.filter` method; the body reads a `LET` of the statement through an object literal field (`E >= { k: $m }.k`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
LET $m = 8;
RETURN [1, 5, 10, 15, 20].filter(|$e| $e >= { k: $m }.k);
