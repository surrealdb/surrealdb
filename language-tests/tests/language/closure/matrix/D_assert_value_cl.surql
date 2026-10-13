/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w ASSERT array::all($value, |$e| $e >= 1)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [1, 5, 10, 15, 20] }]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"
*/
DEFINE FIELD s ON w ASSERT array::all($value, |$e| $e >= 1);
CREATE w:1 SET s = [1, 5, 10, 15, 20];
SELECT VALUE s FROM w;
