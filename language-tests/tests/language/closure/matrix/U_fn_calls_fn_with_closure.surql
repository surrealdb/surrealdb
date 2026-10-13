/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::g($arr: array, $t: any) { RETURN array::filter($arr, |$e| $e >= $t); }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::g($arr: array, $t: any) { RETURN array::filter($arr, |$e| $e >= $t); };
DEFINE FUNCTION fn::h($t: any) { RETURN fn::g([1, 5, 10, 15, 20], $t); };
RETURN fn::h(8);
