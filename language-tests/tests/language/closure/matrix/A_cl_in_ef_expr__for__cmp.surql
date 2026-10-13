/**
[test]
reason = "Position: a closure inside an element filter of an array literal; the body reads a `FOR` variable through a comparison (`E >= $m`). It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20]]"
*/
FOR $m IN [8] { CREATE out SET r = ([1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)]); };
SELECT VALUE r FROM out;
