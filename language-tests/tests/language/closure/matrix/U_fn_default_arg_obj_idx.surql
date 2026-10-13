/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f($o: object) { RETURN array::filter([1, 5, 10, 15, 20], |$e| $e >= $o.v); }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::f($o: object) { RETURN array::filter([1, 5, 10, 15, 20], |$e| $e >= $o.v); };
RETURN fn::f({ v: 8 });
