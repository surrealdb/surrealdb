/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w VALUE $value.a.filter(|$e| $e >= $m)`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

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
DEFINE FIELD s ON w VALUE $value.a.filter(|$e| $e >= $m);
CREATE w:1 SET s = { a: [1, 5, 10] };
SELECT VALUE s FROM w;
