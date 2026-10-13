/**
[test]
reason = "Position: a closure inside an element filter over an array of objects; the body reads a `DEFINE PARAM` through a comparison (`E >= $m`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ s: [1, 5, 10] }, { s: [15, 20] }]"
*/
DEFINE PARAM $m VALUE 8;
RETURN [{ s: [1, 5, 10] }, { s: [15, 20] }][WHERE array::any(s, |$e| $e >= $m)];
