/**
[test]
reason = "The body is written in `RELATE a:1->r:1->b:1 SET v = [1, 10].filter(|$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: r:1, in: a:1, out: b:1, v: [10] }]"

[[test.results]]
value = "[[10]]"
*/
LET $m = 8;
RELATE a:1->r:1->b:1 SET v = [1, 10].filter(|$e| $e >= $m);
SELECT VALUE v FROM r;
