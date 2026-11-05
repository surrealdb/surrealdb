/**
[test]
reason = "The body is written in `UPDATE x:1 SET v[WHERE $this >= $m] = 0`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ id: x:1, v: [1, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: [1, 0] }]"

[[test.results]]
value = "[[1, 0]]"
*/
CREATE x:1 SET v = [1, 10];
LET $m = 8;
UPDATE x:1 SET v[WHERE $this >= $m] = 0;
SELECT VALUE v FROM x:1;
