/**
[test]
reason = "The body is written in `SELECT VALUE a.b.filter(|$e| $e >= $m) FROM [{ a: { b: [1, 5, 10] } }]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[5, 10]]"
*/
LET $m = 5;
SELECT VALUE a.b.filter(|$e| $e >= $m) FROM [{ a: { b: [1, 5, 10] } }];
