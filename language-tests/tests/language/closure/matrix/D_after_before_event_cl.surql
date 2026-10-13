/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON w WHEN true THEN { CREATE log SET v = array::filter($after.s, |$e| $e >= 8) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [1, 5, 10, 15, 20] }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE EVENT ev ON w WHEN true THEN { CREATE log SET v = array::filter($after.s, |$e| $e >= 8) };
CREATE w:1 SET s = [1, 5, 10, 15, 20];
SELECT VALUE v FROM log ORDER BY v;
