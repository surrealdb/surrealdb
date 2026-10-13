/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE score.any(|$e| $e >= $min)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: [95] }]"
*/
LET $min = 90;
RETURN [{score: [95]}, {score: [10]}][WHERE score.any(|$e| $e >= $min)];
