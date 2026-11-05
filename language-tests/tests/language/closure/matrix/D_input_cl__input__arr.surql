/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w VALUE array::filter([1, 5, 10, 15, 20], |$e| $e >= $input[1])`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [10, 15, 20] }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE FIELD s ON w VALUE array::filter([1, 5, 10, 15, 20], |$e| $e >= $input[1]);
CREATE w:1 SET s = [0, 8, 100];
SELECT VALUE s FROM w;
