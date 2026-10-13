/**
[test]
reason = "The body is written in `RETURN [{score: [95]}, {score: [10]}][WHERE array::len(score) >= 1 AND $min = 90]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: [95] }, { score: [10] }]"
*/
LET $min = 90;
RETURN [{score: [95]}, {score: [10]}][WHERE array::len(score) >= 1 AND $min = 90];
