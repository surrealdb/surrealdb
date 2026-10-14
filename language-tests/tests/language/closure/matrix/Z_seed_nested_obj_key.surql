/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= $min)].len()`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"
*/
LET $min = 90;
RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= $min)].len();
