/**
[test]
reason = "Position: the closure of `array::fold`; the body reads a `FOR` variable through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"
*/
FOR $m IN [8] { CREATE out SET r = (array::fold([1, 5, 10, 15, 20], 0, |$a, $e| IF $e >= $m { $a + 1 } ELSE { $a })); };
SELECT VALUE r FROM out;
