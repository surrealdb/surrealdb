/**
[test]
reason = "The body is written in `SELECT VALUE ->rel[WHERE array::any([w], |$e| $e >= $m)].w FROM a:1`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: b:3 }]"

[[test.results]]
value = "[{ id: b:4 }]"

[[test.results]]
value = "[{ id: b:5 }]"

[[test.results]]
value = "[{ id: rel:1, in: a:1, out: b:1, w: 1 }]"

[[test.results]]
value = "[{ id: rel:2, in: a:1, out: b:2, w: 5 }]"

[[test.results]]
value = "[{ id: rel:3, in: a:1, out: b:3, w: 10 }]"

[[test.results]]
value = "[{ id: rel:4, in: a:1, out: b:4, w: 15 }]"

[[test.results]]
value = "[{ id: rel:5, in: a:1, out: b:5, w: 20 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20]]"
*/
CREATE a:1;
CREATE b:1;
CREATE b:2;
CREATE b:3;
CREATE b:4;
CREATE b:5;
RELATE a:1->rel:1->b:1 SET w = 1;
RELATE a:1->rel:2->b:2 SET w = 5;
RELATE a:1->rel:3->b:3 SET w = 10;
RELATE a:1->rel:4->b:4 SET w = 15;
RELATE a:1->rel:5->b:5 SET w = 20;
LET $m = 8;
SELECT VALUE ->rel[WHERE array::any([w], |$e| $e >= $m)].w FROM a:1;
