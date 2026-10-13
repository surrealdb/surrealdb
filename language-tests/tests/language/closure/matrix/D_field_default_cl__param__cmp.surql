/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w DEFAULT array::filter([1, 5, 10, 15, 20], |$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

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
DEFINE FIELD f ON w DEFAULT array::filter([1, 5, 10, 15, 20], |$e| $e >= $m);
CREATE w:1;
SELECT VALUE f FROM w;
