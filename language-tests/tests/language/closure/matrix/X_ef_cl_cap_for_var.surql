/**
[test]
reason = "The body is written in `FOR $m IN [8] { CREATE out SET v = [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)]; }`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10]]"
*/
FOR $m IN [8] { CREATE out SET v = [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)]; };
SELECT VALUE v FROM out;
