/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f() { RETURN $m; }`. A function body reads its arguments, the row names and the request's parameters, never the `LET` of the statement that calls it, so `$m` is unbound in the body and reads as NONE."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/
LET $m = 8;
DEFINE FUNCTION fn::f() { RETURN $m; };
RETURN fn::f();
