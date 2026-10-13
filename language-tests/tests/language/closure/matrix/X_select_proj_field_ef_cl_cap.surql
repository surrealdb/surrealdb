/**
[test]
reason = "The body is written in `SELECT v[WHERE array::any([$this], |$e| $e >= $m)] AS r FROM x:1`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[{ id: x:1, v: [1, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ r: [10] }]"
*/
CREATE x:1 SET v = [1, 10];
LET $m = 8;
SELECT v[WHERE array::any([$this], |$e| $e >= $m)] AS r FROM x:1;
