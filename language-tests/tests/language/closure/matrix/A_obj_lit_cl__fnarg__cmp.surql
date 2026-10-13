/**
[test]
reason = "Position: an object literal that holds a closure and an element filter; the body reads an argument of the enclosing `DEFINE FUNCTION` through a comparison (`E >= $m`). Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: [10, 15, 20], b: [10, 15, 20] }"
*/
DEFINE FUNCTION fn::f($m: any) { RETURN { a: [1, 5, 10, 15, 20].filter(|$e| $e >= $m), b: [1, 5, 10, 15, 20][WHERE $this >= $m] }; };
RETURN fn::f(8);
