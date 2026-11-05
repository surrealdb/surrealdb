/**
[test]
reason = "The body is written in `RETURN fn::g(|$e| $e >= $m)`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"
*/
DEFINE FUNCTION fn::g($f: any) { RETURN $f(10); };
LET $m = 8;
RETURN fn::g(|$e| $e >= $m);
