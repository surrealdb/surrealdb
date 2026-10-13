/**
[test]
reason = "The body is written in `RETURN [{score: [95], k: 90}, {score: [10], k: 90}][WHERE array::any(score, |$e| $e >= k)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[{ k: 90, score: [95] }]"
*/
RETURN [{score: [95], k: 90}, {score: [10], k: 90}][WHERE array::any(score, |$e| $e >= k)];
