/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE array::len(array::filter(score, |$e| $e >= $min)) > 0]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: [95] }]"
*/
LET $min = 90;
RETURN [{score: [95]}, {score: [10]}][WHERE array::len(array::filter(score, |$e| $e >= $min)) > 0];
