/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= $min AND $e <= $max)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: [95] }]"
*/
LET $min = 90;
LET $max = 100;
RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= $min AND $e <= $max)];
