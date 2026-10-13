/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE score[0] >= $min]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: [95] }]"
*/
LET $min = 90;
RETURN [{score: [95]}, {score: [10]}][WHERE score[0] >= $min];
