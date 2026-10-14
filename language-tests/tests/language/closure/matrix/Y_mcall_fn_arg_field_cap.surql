/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f($o: object, $m: number) { RETURN $o.a.filter(|$e| $e >= $m); }`. It runs and reads the names of the frame where the closure is written and the row of the frame that invokes it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 10]"
*/
DEFINE FUNCTION fn::f($o: object, $m: number) { RETURN $o.a.filter(|$e| $e >= $m); };
RETURN fn::f({ a: [1, 5, 10] }, 5);
