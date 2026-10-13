/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20][WHERE $this >= $m] }`. A `LET` of the statement that defines or runs a stored clause is not visible in the clause's own frame, so the clause reads the name as unbound."

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
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 8 }]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"
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
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20][WHERE $this >= $m] };
CREATE src:1 SET lim = 8;
SELECT VALUE v FROM log ORDER BY v;
