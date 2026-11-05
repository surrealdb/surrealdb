/**
[test]
reason = "The body is written in `SELECT VALUE (SELECT VALUE $parent.v.filter(|$e| $e >= $m) FROM [1]) FROM x:1`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[{ id: x:1, v: [1, 5, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[[5, 10]]]"
*/
CREATE x:1 SET v = [1, 5, 10];
LET $m = 5;
SELECT VALUE (SELECT VALUE $parent.v.filter(|$e| $e >= $m) FROM [1]) FROM x:1;
