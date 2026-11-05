/**
[test]
reason = "The body is written in `DEFINE FIELD s.* ON w VALUE array::any([$value], |$e| $e >= 8)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [false, false, true, true, true] }]"

[[test.results]]
value = "[[false, false, true, true, true]]"
*/
DEFINE FIELD s ON w TYPE array;
DEFINE FIELD s.* ON w VALUE array::any([$value], |$e| $e >= 8);
CREATE w:1 SET s = [1, 5, 10, 15, 20];
SELECT VALUE s FROM w;
