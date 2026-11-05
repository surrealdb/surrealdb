/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w VALUE array::filter([1, 5, 10, 15, 20], |$e| $e >= [0, 8, 100][$value])`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [10, 15, 20] }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE FIELD s ON w VALUE array::filter([1, 5, 10, 15, 20], |$e| $e >= [0, 8, 100][$value]);
CREATE w:1 SET s = 1;
SELECT VALUE s FROM w;
