/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20].filter(|$e| $e >= $this.lim) }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 8 }]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"
*/
DEFINE EVENT ev ON src WHEN true THEN { CREATE log SET v = [1, 5, 10, 15, 20].filter(|$e| $e >= $this.lim) };
CREATE src:1 SET lim = 8;
SELECT VALUE v FROM log ORDER BY v;
