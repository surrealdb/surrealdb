/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = $after.a.filter(|$e| $e >= 5) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [1, 5, 10], id: src:1 }]"

[[test.results]]
value = "[[5, 10]]"
*/
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = $after.a.filter(|$e| $e >= 5) };
CREATE src:1 SET a = [1, 5, 10];
SELECT VALUE v FROM log;
