/**
[test]
reason = "Position: an element filter `[WHERE ..]` of an array literal; the body reads a `FOR` variable through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20]]"
*/
FOR $m IN [8] { CREATE out SET r = ([1, 5, 10, 15, 20][WHERE array::any([$this], |$q| $q >= $m)]); };
SELECT VALUE r FROM out;
