/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w VALUE $value.a.filter(|$e| $e >= $m)`. A closure passed to a method called on a receiver path with a field step runs. The field clause reads `$value`, and a LET of the caller is not visible in the clause's frame."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1, s: [1, 5, 10] }]"

[[test.results]]
value = "[[1, 5, 10]]"
*/
LET $m = 5;
DEFINE FIELD s ON w VALUE $value.a.filter(|$e| $e >= $m);
CREATE w:1 SET s = { a: [1, 5, 10] };
SELECT VALUE s FROM w;
