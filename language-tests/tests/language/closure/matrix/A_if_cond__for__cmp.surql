/**
[test]
reason = "Position: the condition of an `IF` expression that holds a closure; the body reads a `FOR` variable through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "['y']"
*/
FOR $m IN [8] { CREATE out SET r = (IF array::any([1, 5, 10, 15, 20], |$e| $e >= $m) { 'y' } ELSE { 'n' }); };
SELECT VALUE r FROM out;
