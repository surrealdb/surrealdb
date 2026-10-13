/**
[test]
reason = "Position: a closure inside an element filter over an array of objects; the body reads a `LET` of the statement through an object literal field (`E >= { k: $m }.k`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ s: [1, 5, 10] }, { s: [15, 20] }]"
*/
LET $m = 8;
RETURN [{ s: [1, 5, 10] }, { s: [15, 20] }][WHERE array::any(s, |$e| $e >= { k: $m }.k)];
