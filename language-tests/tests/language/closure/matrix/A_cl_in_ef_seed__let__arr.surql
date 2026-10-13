/**
[test]
reason = "Position: a closure inside an element filter over an array of objects; the body reads a `LET` of the statement through an array index (`E >= $m[1]`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ s: [1, 5, 10] }, { s: [15, 20] }]"
*/
LET $m = [0, 8, 100];
RETURN [{ s: [1, 5, 10] }, { s: [15, 20] }][WHERE array::any(s, |$e| $e >= $m[1])];
