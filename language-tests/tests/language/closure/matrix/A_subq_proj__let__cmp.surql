/**
[test]
reason = "Position: a subquery `WHERE` in a projected column; the body reads a `LET` of the statement through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[{ id: t:1, lim: 8, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, lim: 8, n: 5 }]"

[[test.results]]
value = "[{ id: t:3, lim: 8, n: 10 }]"

[[test.results]]
value = "[{ id: t:4, lim: 8, n: 15 }]"

[[test.results]]
value = "[{ id: t:5, lim: 8, n: 20 }]"

[[test.results]]
value = "[{ id: u:1, m: 1 }]"

[[test.results]]
value = "[{ id: u:2, m: 5 }]"

[[test.results]]
value = "[{ id: u:3, m: 10 }]"

[[test.results]]
value = "[{ id: u:4, m: 15 }]"

[[test.results]]
value = "[{ id: u:5, m: 20 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 1, r: [10, 15, 20] }]"
*/
CREATE t:1 SET n = 1, lim = 8;
CREATE t:2 SET n = 5, lim = 8;
CREATE t:3 SET n = 10, lim = 8;
CREATE t:4 SET n = 15, lim = 8;
CREATE t:5 SET n = 20, lim = 8;
CREATE u:1 SET m = 1;
CREATE u:2 SET m = 5;
CREATE u:3 SET m = 10;
CREATE u:4 SET m = 15;
CREATE u:5 SET m = 20;
LET $m = 8;
SELECT n, (SELECT VALUE m FROM u WHERE m >= $m ORDER BY m) AS r FROM t:1;
