/**
[test]
reason = "Two functions that call each other run, recursion and all, to their base case."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true, false, false]"

*/

DEFINE FUNCTION fn::even($n: int) { IF $n == 0 { RETURN true }; RETURN fn::odd($n - 1); };
DEFINE FUNCTION fn::odd($n: int) { IF $n == 0 { RETURN false }; RETURN fn::even($n - 1); };
[fn::even(10), fn::odd(7), fn::even(7), fn::odd(0)];
