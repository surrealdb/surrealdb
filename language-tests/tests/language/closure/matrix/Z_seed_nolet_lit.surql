/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= 90)]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ score: [95] }]"
*/
RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= 90)];
