/**
[test]
reason = "The body is written in `SELECT VALUE id FROM x WHERE array::any(score, |$e| $e >= $m) ORDER BY id`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, score: [95] }]"

[[test.results]]
value = "[{ id: x:2, score: [10] }]"

[[test.results]]
value = "[x:1, x:2]"
*/
LET $m = 8;
DEFINE INDEX ix ON x FIELDS score;
CREATE x:1 SET score = [95];
CREATE x:2 SET score = [10];
SELECT VALUE id FROM x WHERE array::any(score, |$e| $e >= $m) ORDER BY id;
