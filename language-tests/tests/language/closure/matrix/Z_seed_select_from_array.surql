/**
[test]
reason = "The body is written in `SELECT VALUE score FROM [{score: [95]}, {score: [10]}] WHERE array::any(score, |$e| $e >= $min)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[95]]"
*/
LET $min = 90;
SELECT VALUE score FROM [{score: [95]}, {score: [10]}] WHERE array::any(score, |$e| $e >= $min);
