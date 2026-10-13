/**
[test]
reason = "The body is written in `CREATE x:1 CONTENT { v: [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)] }`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: [10] }]"

[[test.results]]
value = "[[10]]"
*/
LET $m = 8;
CREATE x:1 CONTENT { v: [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)] };
SELECT VALUE v FROM x:1;
