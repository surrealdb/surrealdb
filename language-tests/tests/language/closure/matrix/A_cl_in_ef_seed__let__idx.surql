/**
[test]
reason = "Position: a closure inside an element filter over an array of objects; the body reads a `LET` of the statement through an index argument (`E >= [0, 8, 100][$m]`). It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ s: [1, 5, 10] }, { s: [15, 20] }]"
*/
LET $m = 1;
RETURN [{ s: [1, 5, 10] }, { s: [15, 20] }][WHERE array::any(s, |$e| $e >= [0, 8, 100][$m])];
