/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= 8)] }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1 }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= 8)] };
CREATE src:1;
SELECT VALUE v FROM log;
