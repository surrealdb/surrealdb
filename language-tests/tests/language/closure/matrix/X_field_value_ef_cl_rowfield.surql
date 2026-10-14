/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= lim)]`. It runs and reads the names of the frame where the closure is written and, for a bare field, the element of the element filter that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [1, 5, 10, 15, 20], id: w:1, lim: 8 }]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"
*/
DEFINE FIELD f ON w VALUE [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= lim)];
CREATE w:1 SET lim = 8;
SELECT VALUE f FROM w;
