/**
[test]
reason = "The body is written in `DEFINE TABLE y AS SELECT v FROM x WHERE array::any([v], |$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: 10 }]"

[[test.results]]
value = "[{ id: x:2, v: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
LET $m = 8;
CREATE x:1 SET v = 10;
CREATE x:2 SET v = 1;
DEFINE TABLE y AS SELECT v FROM x WHERE array::any([v], |$e| $e >= $m);
SELECT VALUE v FROM y;
