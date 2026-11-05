/**
[test]
reason = "The body is written in `DEFINE FUNCTION fn::f() { RETURN [1, 10].map(|$e| $e + $m); }`. A function body reads its arguments, the row names and the request's parameters, never the `LET` of the statement that calls it, so `$m` is unbound in the body, and the closure it writes, which captures `$m` where it is written, adds NONE to each element."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'int' and 'none'"
*/
LET $m = 8;
DEFINE FUNCTION fn::f() { RETURN [1, 10].map(|$e| $e + $m); };
RETURN fn::f();
