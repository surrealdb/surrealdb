/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN $before != NONE THEN { CREATE log SET v = [1, 5, 10, 15, 20].filter(|$e| $e >= $before.lim) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 8 }]"

[[test.results]]
value = "[{ id: src:1, lim: 8, x: 1 }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE EVENT ev ON src WHEN $before != NONE THEN { CREATE log SET v = [1, 5, 10, 15, 20].filter(|$e| $e >= $before.lim) };
CREATE src:1 SET lim = 8;
UPDATE src:1 SET x = 1;
SELECT VALUE v FROM log ORDER BY v;
