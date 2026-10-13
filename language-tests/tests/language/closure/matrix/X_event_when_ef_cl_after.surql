/**
[test]
reason = "The body is written in `DEFINE EVENT ev ON src WHEN [$after.lim][WHERE array::any([$this], |$e| $e >= 8)] != [] THEN { CREATE log SET v = 1 }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, lim: 10 }]"

[[test.results]]
value = "[{ id: src:2, lim: 1 }]"

[[test.results]]
value = "[1]"
*/
DEFINE EVENT ev ON src WHEN [$after.lim][WHERE array::any([$this], |$e| $e >= 8)] != [] THEN { CREATE log SET v = 1 };
CREATE src:1 SET lim = 10;
CREATE src:2 SET lim = 1;
SELECT VALUE v FROM log;
