/**
[test]
reason = "The body is written in `FOR $i IN [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)] { CREATE out SET v = $i; }`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10]"
*/
LET $m = 8;
FOR $i IN [1, 5, 10][WHERE array::any([$this], |$e| $e >= $m)] { CREATE out SET v = $i; };
SELECT VALUE v FROM out;
