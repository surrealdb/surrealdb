/**
[test]
reason = "The body is written in `SELECT VALUE a FROM [{ a: [1, 10] }] WHERE a[WHERE $this >= $m] != []`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 10]]"
*/
LET $m = 8;
SELECT VALUE a FROM [{ a: [1, 10] }] WHERE a[WHERE $this >= $m] != [];
