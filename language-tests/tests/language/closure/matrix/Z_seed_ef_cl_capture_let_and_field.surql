/**
[test]
reason = "The body is written in `RETURN [{score: [95], k: 1}, {score: [10], k: 1}][WHERE array::any(score, |$e| $e >= $min + k)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ k: 1, score: [95] }]"
*/
LET $min = 90;
RETURN [{score: [95], k: 1}, {score: [10], k: 1}][WHERE array::any(score, |$e| $e >= $min + k)];
