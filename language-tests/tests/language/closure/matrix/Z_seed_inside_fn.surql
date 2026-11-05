/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f($min: number) { RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= $min)]; }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ score: [95] }]"
*/
DEFINE FUNCTION fn::f($min: number) { RETURN [{score: [95]}, {score: [10]}][WHERE array::any(score, |$e| $e >= $min)]; };
RETURN fn::f(90);
