/**
[test]
reason = "The body is written in `RETURN fn::g([1, 5, 10, 15, 20], |$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::g($arr: array, $f: any) { RETURN array::filter($arr, $f); };
LET $m = 8;
RETURN fn::g([1, 5, 10, 15, 20], |$e| $e >= $m);
