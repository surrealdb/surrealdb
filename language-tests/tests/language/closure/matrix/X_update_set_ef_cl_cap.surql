/**
[test]
reason = "The body is written in `UPDATE x:1 SET v = v[WHERE array::any([$this], |$e| $e >= $m)]`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "[{ id: x:1, v: [1, 10] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: [10] }]"

[[test.results]]
value = "[[10]]"
*/
CREATE x:1 SET v = [1, 10];
LET $m = 8;
UPDATE x:1 SET v = v[WHERE array::any([$this], |$e| $e >= $m)];
SELECT VALUE v FROM x:1;
