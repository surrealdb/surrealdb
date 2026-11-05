/**
[test]
reason = "The body is written in `SELECT v[WHERE $this >= $m] AS r FROM x:1`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ id: x:1, v: [1, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ r: [10] }]"
*/
CREATE x:1 SET v = [1, 10];
LET $m = 8;
SELECT v[WHERE $this >= $m] AS r FROM x:1;
