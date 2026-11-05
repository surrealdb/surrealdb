/**
[test]
reason = "Position: a closure of `array::filter` nested in a closure of `array::map`; the body reads a `FOR` variable through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[[], [10, 15, 20]]]"
*/
FOR $m IN [8] { CREATE out SET r = (array::map([[1, 5], [10, 15, 20]], |$r| array::filter($r, |$e| $e >= $m))); };
SELECT VALUE r FROM out;
