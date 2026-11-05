/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::r($arr: array, $t: any) { RETURN array::map($arr, |$e| array::len(array::filter($arr, |$x| $x >= $t))); }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 1]"
*/
DEFINE FUNCTION fn::r($arr: array, $t: any) { RETURN array::map($arr, |$e| array::len(array::filter($arr, |$x| $x >= $t))); };
RETURN fn::r([1, 10], 5);
