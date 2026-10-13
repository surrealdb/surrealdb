/**
[test]
reason = "The body is written in `SELECT VALUE a[WHERE array::any([$this], |$e| $e >= $m)] FROM [{ a: [1, 10] }]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10]]"
*/
LET $m = 8;
SELECT VALUE a[WHERE array::any([$this], |$e| $e >= $m)] FROM [{ a: [1, 10] }];
