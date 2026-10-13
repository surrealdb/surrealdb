/**
[test]
reason = "The body is written in `SELECT VALUE a.filter(|$e| $e >= $m) FROM [{ a: [1, 5, 10] }]`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[5, 10]]"
*/
LET $m = 5;
SELECT VALUE a.filter(|$e| $e >= $m) FROM [{ a: [1, 5, 10] }];
