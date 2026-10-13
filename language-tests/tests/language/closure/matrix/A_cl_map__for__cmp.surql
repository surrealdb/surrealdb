/**
[test]
reason = "Position: the closure of `array::map`; the body reads a `FOR` variable through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[false, false, true, true, true]]"
*/
FOR $m IN [8] { CREATE out SET r = (array::map([1, 5, 10, 15, 20], |$e| $e >= $m)); };
SELECT VALUE r FROM out;
