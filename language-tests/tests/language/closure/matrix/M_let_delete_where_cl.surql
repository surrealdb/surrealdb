/**
[test]
reason = "The body is written in `DELETE x WHERE array::any([v], |$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: 10 }]"

[[test.results]]
value = "[{ id: x:2, v: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[x:2]"
*/
LET $m = 8;
CREATE x:1 SET v = 10;
CREATE x:2 SET v = 1;
DELETE x WHERE array::any([v], |$e| $e >= $m);
SELECT VALUE id FROM x;
