/**
[test]
reason = "The body is written in `SELECT VALUE id FROM x WHERE v[WHERE array::any([$this], |$e| $e >= $m)] != [] ORDER BY id`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[{ id: x:1, v: [1, 10] }]"

[[test.results]]
value = "[{ id: x:2, v: [1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[x:1]"
*/
CREATE x:1 SET v = [1, 10];
CREATE x:2 SET v = [1];
LET $m = 8;
SELECT VALUE id FROM x WHERE v[WHERE array::any([$this], |$e| $e >= $m)] != [] ORDER BY id;
