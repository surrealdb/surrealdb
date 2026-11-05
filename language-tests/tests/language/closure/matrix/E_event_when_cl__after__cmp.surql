/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN array::any([$after.n], |$e| $e >= $after.lim) THEN { CREATE log SET v = $after.n }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 8, n: 10 }]"

[[test.results]]
value = "[{ id: src:2, lim: 8, n: 2 }]"

[[test.results]]
value = "[10]"
*/
DEFINE EVENT ev ON src WHEN array::any([$after.n], |$e| $e >= $after.lim) THEN { CREATE log SET v = $after.n };
CREATE src:1 SET n = 10, lim = 8;
CREATE src:2 SET n = 2, lim = 8;
SELECT VALUE v FROM log ORDER BY v;
