/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w COMPUTED [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)]`. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [10, 15, 20], id: w:1 }]"

[[test.results]]
value = "[[10, 15, 20]]"
*/
DEFINE PARAM $m VALUE 8;
DEFINE FIELD f ON w COMPUTED [1, 5, 10, 15, 20][WHERE array::any([$this], |$e| $e >= $m)];
CREATE w:1;
SELECT VALUE f FROM w;
