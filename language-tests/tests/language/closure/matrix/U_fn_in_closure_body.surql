/**
[test]
reason = "The body is written in `RETURN array::filter([1, 5, 10, 15, 20], |$e| fn::ok($e, $m))`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::ok($x: number, $t: number) { RETURN $x >= $t; };
LET $m = 8;
RETURN array::filter([1, 5, 10, 15, 20], |$e| fn::ok($e, $m));
