/**
[test]
reason = "The body is written in `RETURN x:1.v.filter(|$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ id: x:1, v: [1, 5, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
CREATE x:1 SET v = [1, 5, 10];
LET $m = 5;
RETURN x:1.v.filter(|$e| $e >= $m);
