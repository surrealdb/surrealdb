/**
[test]
reason = "The body is written in `INSERT INTO x { id: 1, v: [1, 10].filter(|$e| $e >= $m) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: [10] }]"

[[test.results]]
value = "[[10]]"
*/
LET $m = 8;
INSERT INTO x { id: 1, v: [1, 10].filter(|$e| $e >= $m) };
SELECT VALUE v FROM x:1;
