/**
[test]
reason = "Position: the closure of `array::filter`; the body reads an argument of the enclosing `DEFINE FUNCTION` through a closure nested in the body (`array::any([E], |$q| $q >= $m)`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN array::filter([1, 5, 10, 15, 20], |$e| array::any([$e], |$q| $q >= $m)); };
RETURN fn::f(8);
