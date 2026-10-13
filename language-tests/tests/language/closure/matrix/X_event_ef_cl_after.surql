/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $after.lim)] }`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 8 }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $after.lim)] };
CREATE src:1 SET lim = 8;
SELECT VALUE v FROM log;
