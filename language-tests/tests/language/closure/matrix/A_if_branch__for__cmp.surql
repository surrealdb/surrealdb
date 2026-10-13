/**
[test]
reason = "Position: a branch of an `IF` expression that holds a closure; the body reads a `FOR` variable through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[10, 15, 20]]"
*/
FOR $m IN [8] { CREATE out SET r = (IF true { [1, 5, 10, 15, 20].filter(|$e| $e >= $m) } ELSE { [] }); };
SELECT VALUE r FROM out;
