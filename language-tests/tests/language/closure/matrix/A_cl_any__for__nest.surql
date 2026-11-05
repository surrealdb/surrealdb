/**
[test]
reason = "Position: the closure of `array::any`; the body reads a `FOR` variable through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true]"
*/
FOR $m IN [8] { CREATE out SET r = (array::any([1, 5, 10, 15, 20], |$e| array::any([$e], |$q| $q >= $m))); };
SELECT VALUE r FROM out;
