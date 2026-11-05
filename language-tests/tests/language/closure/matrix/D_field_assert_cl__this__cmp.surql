/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w ASSERT array::any([1, 5, 10, 15, 20], |$e| $e >= $this.lim)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, lim: 8 }]"

[[test.results]]
value = "[NONE]"
*/
DEFINE FIELD f ON w ASSERT array::any([1, 5, 10, 15, 20], |$e| $e >= $this.lim);
CREATE w:1 SET lim = 8;
SELECT VALUE f FROM w;
