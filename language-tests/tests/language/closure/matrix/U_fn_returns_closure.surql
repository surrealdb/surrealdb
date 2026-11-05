/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::mk($t: any) { RETURN |$e| $e >= $t; }`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 15, 20]"
*/
DEFINE FUNCTION fn::mk($t: any) { RETURN |$e| $e >= $t; };
LET $f = fn::mk(8);
RETURN array::filter([1, 5, 10, 15, 20], $f);
