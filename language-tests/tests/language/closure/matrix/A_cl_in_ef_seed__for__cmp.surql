/**
[test]
reason = "Position: a closure inside an element filter over an array of objects; the body reads a `FOR` variable through a comparison (`E >= $m`). It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[{ s: [1, 5, 10] }, { s: [15, 20] }]]"
*/
FOR $m IN [8] { CREATE out SET r = ([{ s: [1, 5, 10] }, { s: [15, 20] }][WHERE array::any(s, |$e| $e >= $m)]); };
SELECT VALUE r FROM out;
