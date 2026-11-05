/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w VALUE $value.filter(|$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [5, 10] }]"

[[test.results]]
value = "[[5, 10]]"
*/
DEFINE PARAM $m VALUE 5;
DEFINE FIELD s ON w VALUE $value.filter(|$e| $e >= $m);
CREATE w:1 SET s = [1, 5, 10];
SELECT VALUE s FROM w;
