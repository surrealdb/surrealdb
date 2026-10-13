/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w COMPUTED array::filter([1, 5, 10, 15, 20], |$e| $e >= lim)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [10, 15, 20], id: w:1, lim: 8 }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE FIELD f ON w COMPUTED array::filter([1, 5, 10, 15, 20], |$e| $e >= lim);
CREATE w:1 SET lim = 8;
SELECT VALUE f FROM w;
